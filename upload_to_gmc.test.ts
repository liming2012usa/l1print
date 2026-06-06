import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { writeFile, mkdtemp } from 'node:fs/promises';
import Database from 'better-sqlite3';
import type { content_v2_1 } from 'googleapis';
import {
  isNotFoundError,
  isNetworkError,
  parseBatchSize,
  parseLimit,
  createRestProductId,
  indexBatchResults,
  buildVariantOfferId,
  hashProduct,
  loadCachedVariants,
  upsertVariantRecord,
  upsertVariantRecords,
  runProductsCustomBatch,
  uploadProducts,
  deleteStaleProducts,
  ensureAbsoluteUrl,
  slugify,
  applyTemplate,
  sanitizeDescription,
  buildTitleWithBrand,
  buildTitleWithColor,
  toArray,
  shouldExcludeSize,
  getEnvVar,
  resolvePath,
  parseCliArgs,
  getRetryDelayMs,
  withTimeout,
  describeVariant,
  logApiError,
  logDuplicateOfferId,
  loadFeedProducts,
  loadMetaDataMaps,
  loadInlineCredentialsFromEnv,
  mapFeedProductToGoogleProduct,
} from './upload_to_gmc.js';
import type {
  MappingOptions,
  UploadQueueItem,
  CachedVariantRecord,
  SqliteDatabase,
  InferenceOptions,
} from './upload_to_gmc.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.join(HERE, 'test_fixtures');
const FIXTURE_PRODUCTS = path.join(FIXTURES, 'products.sample.xml');
const FIXTURE_META = path.join(FIXTURES, 'meta_data.sample.xml');

function fullMapping(): MappingOptions {
  return {
    baseStoreUrl: 'https://l1print.com/',
    assetBaseUrl: 'https://assets.l1print.com/',
    productPathTemplate: '/blank_product/{id}/{nameSlug}',
    contentLanguage: 'en',
    targetCountry: 'US',
    channel: 'online',
    priceCurrency: 'USD',
    defaultAvailability: 'in stock',
    defaultCondition: 'new',
    defaultGpc: '212',
  };
}

// ---------- test helpers ----------

const MAPPING = { channel: 'online', contentLanguage: 'en', targetCountry: 'US' } as unknown as MappingOptions;

type BatchEntry = content_v2_1.Schema$ProductsCustomBatchRequestEntry;
type BatchResult = content_v2_1.Schema$ProductsCustomBatchResponseEntry;

// Builds a fake content API whose custombatch records every call and returns
// whatever the responder produces, so we can drive success/notFound/error paths
// without touching the network.
function makeApi(responder: (entries: BatchEntry[]) => BatchResult[]) {
  const calls: BatchEntry[][] = [];
  const api = {
    products: {
      custombatch: async ({ requestBody }: { requestBody: { entries: BatchEntry[] } }) => {
        calls.push(requestBody.entries);
        return { data: { entries: responder(requestBody.entries) } };
      },
    },
  } as unknown as content_v2_1.Content;
  return { api, calls };
}

function makeDb(): SqliteDatabase {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE product_variants (
      offer_id TEXT PRIMARY KEY,
      item_group_id TEXT,
      hash TEXT NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `);
  return db as unknown as SqliteDatabase;
}

function record(offerId: string, hash = 'h'): CachedVariantRecord {
  return { offerId, itemGroupId: 'g', hash, updatedAt: 1 };
}

function notFoundEntry(batchId: number): BatchResult {
  return { batchId, errors: { code: 404, errors: [{ reason: 'notFound', message: 'gone' }] } };
}

function errorEntry(batchId: number): BatchResult {
  return { batchId, errors: { code: 500, errors: [{ reason: 'internalError', message: 'boom' }] } };
}

async function quiet<T>(fn: () => Promise<T>): Promise<T> {
  const { log, error, warn } = console;
  console.log = console.error = console.warn = () => {};
  try {
    return await fn();
  } finally {
    Object.assign(console, { log, error, warn });
  }
}

// ---------- pure helpers ----------

test('isNotFoundError classifies GMC error shapes', () => {
  assert.equal(isNotFoundError({ errors: [{ reason: 'notFound' }] }), true);
  assert.equal(isNotFoundError({ code: 404, errors: [{ reason: 'notFound' }] }), true);
  assert.equal(isNotFoundError({ code: 404 }), true);
  assert.equal(isNotFoundError({ status: 404 }), true);
  assert.equal(isNotFoundError({ errors: [{ reason: 'internalError' }] }), false);
  assert.equal(isNotFoundError({ code: 500 }), false);
  assert.equal(isNotFoundError(null), false);
  assert.equal(isNotFoundError('notFound'), false);
});

test('isNetworkError matches transient codes and messages only', () => {
  assert.equal(isNetworkError({ code: 'ETIMEDOUT' }), true);
  assert.equal(isNetworkError({ code: 'ECONNRESET' }), true);
  assert.equal(isNetworkError({ message: 'socket hang up' }), true);
  assert.equal(isNetworkError({ message: 'fetch failed' }), true);
  assert.equal(isNetworkError({ code: 'EPERM' }), false);
  assert.equal(isNetworkError({ message: 'bad request' }), false);
  assert.equal(isNetworkError({ errors: [{ reason: 'notFound' }] }), false);
  assert.equal(isNetworkError(undefined), false);
});

test('parseBatchSize parses, floors and caps; rejects junk', () => {
  assert.equal(parseBatchSize('500'), 500);
  assert.equal(parseBatchSize('2000'), 1000); // capped at MAX_BATCH_SIZE
  assert.equal(parseBatchSize('3.9'), 3); // floored
  assert.equal(parseBatchSize('0'), undefined);
  assert.equal(parseBatchSize('abc'), undefined);
  assert.equal(parseBatchSize(undefined), undefined);
});

test('parseLimit accepts positive numbers only', () => {
  assert.equal(parseLimit('5'), 5);
  assert.equal(parseLimit('0'), undefined);
  assert.equal(parseLimit('x'), undefined);
});

test('createRestProductId builds channel:lang:country:offerId', () => {
  assert.equal(createRestProductId('ABC-one size', MAPPING), 'online:en:US:ABC-one size');
});

test('indexBatchResults maps by batchId, skipping entries without one', () => {
  const m = indexBatchResults([
    { batchId: 0 },
    { batchId: 2 },
    {} as BatchResult,
  ]);
  assert.equal(m.size, 2);
  assert.ok(m.has(0));
  assert.ok(m.has(2));
});

test('buildVariantOfferId normalizes color but keeps raw lowercased size', () => {
  // Documents current behavior: color is slugified, size is only lowercased
  // (so "One Size" keeps its space). GMC stores this verbatim, verified live.
  assert.equal(buildVariantOfferId('C1', 'Navy Reflective', 'One Size'), 'C1-navy-reflective-one size');
  assert.equal(buildVariantOfferId('C1', 'Red', 'M'), 'C1-red-m');
  assert.equal(buildVariantOfferId('C1', undefined, undefined), 'C1-variant');
});

test('hashProduct is deterministic and sensitive to content', () => {
  const p = { offerId: 'A', title: 't', color: 'Red' } as content_v2_1.Schema$Product;
  assert.equal(hashProduct(p), hashProduct({ ...p }));
  assert.notEqual(hashProduct(p), hashProduct({ ...p, title: 't2' }));
});

// ---------- runProductsCustomBatch ----------

test('runProductsCustomBatch returns entries on success', async () => {
  const { api } = makeApi(() => [{ batchId: 0 }]);
  const res = await runProductsCustomBatch(api, [{ batchId: 0, merchantId: 'M', method: 'insert' }], 'x');
  assert.deepEqual(res, [{ batchId: 0 }]);
});

test('runProductsCustomBatch returns null on non-network failure (no retry loop)', async () => {
  const api = {
    products: {
      custombatch: async () => {
        throw { code: 429, errors: [{ reason: 'quotaExceeded' }] };
      },
    },
  } as unknown as content_v2_1.Content;
  const res = await quiet(() => runProductsCustomBatch(api, [{ batchId: 0 }], 'x'));
  assert.equal(res, null);
});

// ---------- uploadProducts ----------

test('uploadProducts batches by size and records every success', async () => {
  const db = makeDb();
  const { api, calls } = makeApi((entries) => entries.map((e) => ({ batchId: e.batchId as number })));
  const items: UploadQueueItem[] = [
    { product: { offerId: 'A', title: 'a' }, hash: 'h1' },
    { product: { offerId: 'B', title: 'b' }, hash: 'h2' },
    { product: { offerId: 'C', title: 'c' }, hash: 'h3' },
  ];

  await quiet(() => uploadProducts(items, 'M', api, db, false, 2));

  assert.deepEqual(calls.map((c) => c.length), [2, 1]); // chunked 2 + 1
  const cached = loadCachedVariants(db);
  assert.equal(cached.size, 3);
  assert.equal(cached.get('A')?.hash, 'h1');
  db.close();
});

test('uploadProducts records only successful entries, not failed ones', async () => {
  const db = makeDb();
  const { api } = makeApi((entries) =>
    entries.map((e) =>
      (e.product?.offerId === 'B' ? errorEntry(e.batchId as number) : { batchId: e.batchId as number })),
  );
  const items: UploadQueueItem[] = [
    { product: { offerId: 'A' }, hash: 'h1' },
    { product: { offerId: 'B' }, hash: 'h2' },
    { product: { offerId: 'C' }, hash: 'h3' },
  ];

  await quiet(() => uploadProducts(items, 'M', api, db, false, 5));

  const cached = loadCachedVariants(db);
  assert.deepEqual([...cached.keys()].sort(), ['A', 'C']);
  db.close();
});

test('uploadProducts dry-run makes no API calls and writes nothing', async () => {
  const db = makeDb();
  const { api, calls } = makeApi(() => []);
  const items: UploadQueueItem[] = [{ product: { offerId: 'A' }, hash: 'h1' }];

  await quiet(() => uploadProducts(items, 'M', api, db, true, 5));

  assert.equal(calls.length, 0);
  assert.equal(loadCachedVariants(db).size, 0);
  db.close();
});

// ---------- deleteStaleProducts ----------

test('deleteStaleProducts purges success and notFound, keeps real errors', async () => {
  const db = makeDb();
  upsertVariantRecords(db, [record('A'), record('B'), record('C')]);

  const { api } = makeApi((entries) =>
    entries.map((e) => {
      const id = String(e.productId);
      if (id.endsWith(':A')) return { batchId: e.batchId as number }; // deleted ok
      if (id.endsWith(':B')) return notFoundEntry(e.batchId as number); // already gone
      return errorEntry(e.batchId as number); // transient/real error -> keep
    }),
  );

  await quiet(() => deleteStaleProducts(['A', 'B', 'C'], 'M', MAPPING, api, db, false, 10));

  const cached = loadCachedVariants(db);
  assert.equal(cached.has('A'), false); // deleted -> purged
  assert.equal(cached.has('B'), false); // notFound -> purged (stops the loop)
  assert.equal(cached.has('C'), true); // error -> retained for next run
  db.close();
});

test('deleteStaleProducts dry-run makes no API calls and purges nothing', async () => {
  const db = makeDb();
  upsertVariantRecords(db, [record('A')]);
  const { api, calls } = makeApi(() => []);

  await quiet(() => deleteStaleProducts(['A'], 'M', MAPPING, api, db, true, 10));

  assert.equal(calls.length, 0);
  assert.equal(loadCachedVariants(db).has('A'), true);
  db.close();
});

// ---------- pure string/url/title helpers ----------

test('slugify normalizes to lowercase kebab and strips punctuation', () => {
  assert.equal(slugify('Hello World!'), 'hello-world');
  assert.equal(slugify("Port Authority Women's Polo"), 'port-authority-womens-polo');
  assert.equal(slugify('  multiple   spaces_and-dashes '), 'multiple-spaces-and-dashes');
  assert.equal(slugify(undefined), '');
  assert.equal(slugify(''), '');
});

test('ensureAbsoluteUrl joins relative paths and passes through absolute', () => {
  assert.equal(ensureAbsoluteUrl('https://x.com/', '/a/b.png'), 'https://x.com/a/b.png');
  assert.equal(ensureAbsoluteUrl('https://x.com', 'a/b.png'), 'https://x.com/a/b.png');
  assert.equal(ensureAbsoluteUrl('https://x.com/', 'https://cdn.com/c.png'), 'https://cdn.com/c.png');
  assert.equal(ensureAbsoluteUrl('https://x.com/', undefined), undefined);
});

test('applyTemplate substitutes id/code/nameSlug', () => {
  const out = applyTemplate('/p/{id}/{code}/{nameSlug}', { id: '1', code: 'C9', name: 'Red Shirt' });
  assert.equal(out, '/p/1/C9/red-shirt');
});

test('sanitizeDescription decodes entities, converts markup, drops tags', () => {
  const out = sanitizeDescription('<p>Hello&amp;Co</p><ul><li>One</li><li>Two</li></ul>');
  assert.ok(out.includes('Hello&Co'));
  assert.ok(out.includes('• One'));
  assert.ok(out.includes('• Two'));
  assert.ok(!out.includes('<'));
  assert.equal(sanitizeDescription(undefined), '');
});

test('buildTitleWithBrand prepends brand unless already present', () => {
  assert.equal(buildTitleWithBrand('Polo', 'Nike'), 'Nike Polo');
  assert.equal(buildTitleWithBrand('Nike Polo', 'Nike'), 'Nike Polo');
  assert.equal(buildTitleWithBrand('Polo', undefined), 'Polo');
  assert.equal(buildTitleWithBrand('Polo', '   '), 'Polo');
});

test('buildTitleWithColor appends a single color and "& More Colors" for many', () => {
  const single = buildTitleWithColor('Tee', 'Red', [{ clr: { name: 'Red' } }]);
  assert.equal(single, 'Tee - Red');
  const many = buildTitleWithColor('Tee', undefined, [{ clr: { name: 'Red' } }, { clr: { name: 'Blue' } }]);
  assert.ok(many.includes('& More Colors'));
  assert.equal(buildTitleWithColor('', 'Red', []), '');
  // overlong base falls back to "Multiple Colors" truncation path
  const longBase = 'X'.repeat(200);
  const fallback = buildTitleWithColor(longBase, undefined, [{ clr: { name: 'Red' } }, { clr: { name: 'Blue' } }]);
  assert.ok(fallback.length <= 150);
});

test('toArray normalizes scalar/array/undefined', () => {
  assert.deepEqual(toArray(undefined), []);
  assert.deepEqual(toArray('x'), ['x']);
  assert.deepEqual(toArray(['x', 'y']), ['x', 'y']);
});

test('shouldExcludeSize matches only the oversized tokens', () => {
  assert.equal(shouldExcludeSize('3XL'), true);
  assert.equal(shouldExcludeSize('4xl'), true);
  assert.equal(shouldExcludeSize(' 6XL '), true);
  assert.equal(shouldExcludeSize('M'), false);
  assert.equal(shouldExcludeSize('2XL'), false);
  assert.equal(shouldExcludeSize(''), false);
});

test('describeVariant collapses whitespace and labels attributes', () => {
  const { displayTitle, variantLabel } = describeVariant({ title: '  A   B ', color: 'Red', sizes: ['M'] });
  assert.equal(displayTitle, 'A B');
  assert.ok(variantLabel.includes('color: Red'));
  assert.ok(variantLabel.includes('size: M'));
  const fallback = describeVariant({});
  assert.ok(fallback.displayTitle === 'n/a');
});

// ---------- env / cli / path helpers ----------

test('getEnvVar returns value, fallback, or throws', () => {
  process.env.__TEST_VAR__ = '  hello  ';
  assert.equal(getEnvVar('__TEST_VAR__'), 'hello');
  delete process.env.__TEST_VAR__;
  assert.equal(getEnvVar('__TEST_VAR__', 'fb'), 'fb');
  assert.throws(() => getEnvVar('__TEST_VAR__'));
});

test('resolvePath handles absolute, relative and default', () => {
  assert.equal(resolvePath('/abs/x.xml'), '/abs/x.xml');
  assert.equal(resolvePath('rel.xml'), path.resolve(process.cwd(), 'rel.xml'));
  assert.ok(path.isAbsolute(resolvePath(undefined)));
});

test('parseCliArgs parses all flags (space and = forms)', () => {
  const a = parseCliArgs(['--xml', '/x.xml', '--meta', '/m.xml', '--dry-run', '--limit', '50', '--batch-size', '200', '--infer-description']);
  assert.equal(a.xmlPath, '/x.xml');
  assert.equal(a.metaPath, '/m.xml');
  assert.equal(a.dryRun, true);
  assert.equal(a.limit, 50);
  assert.equal(a.batchSize, 200);
  assert.equal(a.includeDescriptionInInference, true);

  const b = parseCliArgs(['--xml=/y.xml', '--meta=/n.xml', '--limit=10', '--batch-size=300', '--no-infer-description']);
  assert.equal(b.xmlPath, '/y.xml');
  assert.equal(b.limit, 10);
  assert.equal(b.batchSize, 300);
  assert.equal(b.includeDescriptionInInference, false);

  const c = parseCliArgs([]);
  assert.equal(c.dryRun, false);
  assert.equal(c.batchSize, 500); // default
});

test('getRetryDelayMs grows exponentially and caps', () => {
  assert.equal(getRetryDelayMs(0), 10_000);
  assert.equal(getRetryDelayMs(1), 20_000);
  assert.equal(getRetryDelayMs(2), 40_000);
  assert.equal(getRetryDelayMs(50), 120_000); // capped at RETRY_MAX_DELAY_MS
});

test('withTimeout resolves fast promises and rejects slow ones', async () => {
  assert.equal(await withTimeout(Promise.resolve(42), 1000, 'fast'), 42);
  await assert.rejects(
    () => withTimeout(new Promise(() => {}), 5, 'slow'),
    /timed out/,
  );
});

// ---------- logging helpers (coverage: must not throw) ----------

test('logApiError handles structured, Error and primitive inputs', async () => {
  await quiet(async () => {
    logApiError('ctx', { errors: [{ reason: 'notFound', message: 'gone' }] });
    logApiError('ctx', new Error('boom'));
    logApiError('ctx', 'weird');
  });
  assert.ok(true);
});

test('logDuplicateOfferId formats a line', async () => {
  await quiet(async () => {
    logDuplicateOfferId({ offerId: 'A', itemGroupId: 'g', color: 'Red', sizes: ['M'], title: 'T' });
  });
  assert.ok(true);
});

// ---------- credentials ----------

test('loadInlineCredentialsFromEnv: unset, inline JSON, file path, invalid', async () => {
  const original = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  try {
    delete process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
    assert.equal(await loadInlineCredentialsFromEnv(), undefined);

    process.env.GOOGLE_SERVICE_ACCOUNT_JSON = '{"client_email":"svc@x.iam"}';
    assert.deepEqual(await loadInlineCredentialsFromEnv(), { client_email: 'svc@x.iam' });

    const dir = await mkdtemp(path.join(os.tmpdir(), 'gmc-cred-'));
    const file = path.join(dir, 'key.json');
    await writeFile(file, '{"client_email":"file@x.iam"}');
    process.env.GOOGLE_SERVICE_ACCOUNT_JSON = file;
    assert.deepEqual(await loadInlineCredentialsFromEnv(), { client_email: 'file@x.iam' });

    process.env.GOOGLE_SERVICE_ACCOUNT_JSON = '{not valid';
    assert.equal(await quiet(() => loadInlineCredentialsFromEnv()), undefined);
  } finally {
    if (original === undefined) delete process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
    else process.env.GOOGLE_SERVICE_ACCOUNT_JSON = original;
  }
});

// ---------- feed / metadata loading ----------

test('loadMetaDataMaps parses categories and manufacturers; bad path -> empty', async () => {
  const meta = await loadMetaDataMaps(FIXTURE_META);
  assert.ok(Object.keys(meta.categories).length > 0);
  assert.ok(Object.keys(meta.manufacturers).length > 0);

  const empty = await quiet(() => loadMetaDataMaps('/no/such/file.xml'));
  assert.deepEqual(empty, { categories: {}, manufacturers: {} });
});

test('loadFeedProducts reads products; empty doc -> []', async () => {
  const products = await loadFeedProducts(FIXTURE_PRODUCTS);
  assert.ok(products.length >= 5);

  const dir = await mkdtemp(path.join(os.tmpdir(), 'gmc-feed-'));
  const empty = path.join(dir, 'empty.xml');
  await writeFile(empty, '<?xml version="1.0"?><products></products>');
  assert.deepEqual(await loadFeedProducts(empty), []);
});

// ---------- full mapping pipeline (covers color/image/inference helpers) ----------

test('mapFeedProductToGoogleProduct produces valid variants across fixture catalog', async () => {
  const meta = await loadMetaDataMaps(FIXTURE_META);
  const products = await loadFeedProducts(FIXTURE_PRODUCTS);
  const mapping = fullMapping();
  const inference: InferenceOptions = { includeDescription: false };

  let totalVariants = 0;
  const offerIds = new Set<string>();
  for (const product of products) {
    const variants = mapFeedProductToGoogleProduct(product, mapping, meta.categories, meta.manufacturers, inference);
    assert.ok(variants.length > 0, 'each product yields at least one variant');
    for (const v of variants) {
      assert.ok(v.offerId, 'offerId set');
      assert.ok(v.title && v.title.length > 0 && v.title.length <= 150, 'title within GMC limit');
      assert.ok(['male', 'female', 'unisex'].includes(String(v.gender)));
      assert.ok(['newborn', 'infant', 'toddler', 'kids', 'adult'].includes(String(v.ageGroup)));
      assert.equal(v.channel, 'online');
      assert.equal(v.contentLanguage, 'en');
      assert.equal(v.targetCountry, 'US');
      assert.ok(v.link?.startsWith('https://l1print.com/'));
      offerIds.add(String(v.offerId));
    }
    totalVariants += variants.length;
  }
  assert.ok(totalVariants > products.length, 'variants expand beyond raw products');
  assert.ok(offerIds.size > 0);
});

test('mapFeedProductToGoogleProduct with includeDescription inference still maps', async () => {
  const meta = await loadMetaDataMaps(FIXTURE_META);
  const products = await loadFeedProducts(FIXTURE_PRODUCTS);
  const variants = mapFeedProductToGoogleProduct(
    products[0],
    fullMapping(),
    meta.categories,
    meta.manufacturers,
    { includeDescription: true },
  );
  assert.ok(variants.length > 0);
  assert.ok(variants.every((v) => v.offerId));
});

test('mapFeedProductToGoogleProduct handles a minimal product with no colors/sizes', () => {
  const variants = mapFeedProductToGoogleProduct(
    { id: '9', code: 'Z9', name: 'Bare Item', price: '5' },
    fullMapping(),
    {},
    {},
    { includeDescription: false },
  );
  assert.equal(variants.length, 1);
  assert.equal(variants[0].offerId, 'Z9-9-variant');
  assert.equal(variants[0].price?.value, '5.00');
});

// ---------- single-record upsert ----------

test('upsertVariantRecord inserts then updates on conflict', () => {
  const db = makeDb();
  upsertVariantRecord(db, record('A', 'h1'));
  assert.equal(loadCachedVariants(db).get('A')?.hash, 'h1');
  upsertVariantRecord(db, record('A', 'h2'));
  assert.equal(loadCachedVariants(db).get('A')?.hash, 'h2');
  assert.equal(loadCachedVariants(db).size, 1);
  db.close();
});

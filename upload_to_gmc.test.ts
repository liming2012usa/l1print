import test from 'node:test';
import assert from 'node:assert/strict';
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
  upsertVariantRecords,
  runProductsCustomBatch,
  uploadProducts,
  deleteStaleProducts,
} from './upload_to_gmc.js';
import type {
  MappingOptions,
  UploadQueueItem,
  CachedVariantRecord,
  SqliteDatabase,
} from './upload_to_gmc.js';

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

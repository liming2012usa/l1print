import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import DatabaseConstructor from 'better-sqlite3';
import { google, content_v2_1 } from 'googleapis';
import type { JWTInput } from 'google-auth-library';
import { parseStringPromise } from 'xml2js';
import he from 'he';
import dotenv from 'dotenv';

type SchemaProduct = content_v2_1.Schema$Product;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

initializeEnv();

interface CliOptions {
  xmlPath: string;
  metaPath: string;
  dryRun: boolean;
  limit?: number;
  includeDescriptionInInference: boolean;
}

type GenderValue = 'male' | 'female' | 'unisex';
type AgeGroupValue = 'newborn' | 'infant' | 'toddler' | 'kids' | 'adult';

interface MappingOptions {
  baseStoreUrl: string;
  productPathTemplate: string;
  assetBaseUrl: string;
  contentLanguage: string;
  targetCountry: string;
  channel: 'online' | 'local';
  priceCurrency: string;
  defaultAvailability: string;
  defaultCondition: string;
  defaultGpc?: string;
}

interface InferenceOptions {
  includeDescription: boolean;
}

interface FeedProduct {
  id?: string;
  name?: string;
  description?: string;
  code?: string;
  default_color_id?: string;
  price?: string;
  cheapest_price?: string;
  manufacturer_id?: string;
  type_id?: string;
  images?: {
    image?: FeedImage | FeedImage[];
  };
  sizes?: {
    size?: FeedSize | FeedSize[];
  };
  colors?: {
    color?: FeedColor | FeedColor[];
  };
  views?: {
    view?: FeedView | FeedView[];
  };
  categories?: {
    category?: FeedProductCategory | FeedProductCategory[];
  };
  [key: string]: unknown;
}

interface FeedImage {
  type?: string;
  src?: string;
}

interface FeedSize {
  id?: string;
  name?: string;
  value?: string;
  size_label_1?: string;
  size_label_2?: string;
  selected?: string;
}

interface FeedColor {
  id?: string;
  name?: string;
  color_id?: string;
  clr?: FeedClr | FeedClr[];
}

interface FeedClr {
  name?: string;
  html?: string;
}

interface FeedView {
  name?: string;
  image_url?: string;
}

type ImageCategory = 'front' | 'back' | 'other';

interface FeedProductCategory {
  id?: string;
  category_id?: string;
}

interface MetaCategory {
  id?: string;
  name?: string;
  description?: string;
  category?: MetaCategory | MetaCategory[];
}

interface MetaManufacturer {
  id?: string;
  name?: string;
}

interface CachedVariantRecord {
  offerId: string;
  itemGroupId?: string;
  hash: string;
  updatedAt: number;
}

interface UploadQueueItem {
  product: SchemaProduct;
  hash: string;
}

const DEFAULT_XML_PATH = path.resolve(__dirname, 'data_feeds', 'products.xml');
const DEFAULT_META_PATH = path.resolve(__dirname, 'data_feeds', 'meta_data.xml');
const DB_PATH = path.resolve(__dirname, '.cache', 'gmc-sync.db');
const COLOR_ATTRIBUTE_MAX_LENGTH = 100;
const MAX_VARIANT_IMAGE_COUNT = 11;
const PREFERRED_IMAGE_SIZE_ID = '13';
const DEFAULT_GENDER: GenderValue = 'male';
const DEFAULT_KIDS_GENDER: GenderValue = 'unisex';
const DEFAULT_AGE_GROUP: AgeGroupValue = 'adult';

const FORCE_EXIT_ARM_DELAY_MS = 500;
let stopRequested = false;
let forceExitArmed = false;
let forceExitTimer: NodeJS.Timeout | undefined;

function armForceExit() {
  if (forceExitTimer) {
    clearTimeout(forceExitTimer);
  }
  forceExitTimer = setTimeout(() => {
    forceExitArmed = true;
  }, FORCE_EXIT_ARM_DELAY_MS);
}

process.on('SIGINT', () => {
  if (!stopRequested) {
    stopRequested = true;
    forceExitArmed = false;
    armForceExit();
    console.log('\nInterrupt received. Finishing current task before exiting... (press Ctrl+C again to force quit)');
    return;
  }
  if (!forceExitArmed) {
    return;
  }
  console.log('\nForce exiting.');
  process.exit(1);
});

function isStopRequested(): boolean {
  return stopRequested;
}

function initializeEnv() {
  const searchPaths = [
    path.resolve(process.cwd(), '.env'),
    path.resolve(__dirname, '.env'),
    path.resolve(__dirname, '..', '.env'),
  ];
  const seen = new Set<string>();

  for (const envPath of searchPaths) {
    if (seen.has(envPath)) continue;
    seen.add(envPath);
    const result = dotenv.config({ path: envPath });
    if (!result.error) {
      console.log(`Loaded environment variables from ${envPath}`);
      break;
    }
    const error = result.error as NodeJS.ErrnoException | undefined;
    if (error && error.code && error.code !== 'ENOENT') {
      console.warn(`Failed to load environment file at ${envPath}:`, error);
    }
  }
}

function parseCliArgs(args: string[]): CliOptions {
  let xmlPath = DEFAULT_XML_PATH;
  let metaPath = DEFAULT_META_PATH;
  let dryRun = false;
  let limit: number | undefined;
  let includeDescriptionInInference = false;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];

    if (arg === '--xml' && args[i + 1]) {
      xmlPath = resolvePath(args[i + 1]);
      i += 1;
    } else if (arg.startsWith('--xml=')) {
      const [, value] = arg.split('=');
      xmlPath = resolvePath(value);
    } else if (arg === '--meta' && args[i + 1]) {
      metaPath = resolvePath(args[i + 1]);
      i += 1;
    } else if (arg.startsWith('--meta=')) {
      const [, value] = arg.split('=');
      metaPath = resolvePath(value);
    } else if (arg === '--dry-run') {
      dryRun = true;
    } else if (arg === '--infer-description') {
      includeDescriptionInInference = true;
    } else if (arg === '--no-infer-description') {
      includeDescriptionInInference = false;
    } else if (arg === '--limit' && args[i + 1]) {
      limit = parseLimit(args[i + 1]);
      i += 1;
    } else if (arg.startsWith('--limit=')) {
      const [, value] = arg.split('=');
      limit = parseLimit(value);
    }
  }

  return { xmlPath, metaPath, dryRun, limit, includeDescriptionInInference };
}

function resolvePath(inputPath?: string): string {
  if (!inputPath) {
    return DEFAULT_XML_PATH;
  }
  return path.isAbsolute(inputPath)
    ? inputPath
    : path.resolve(process.cwd(), inputPath);
}

function parseLimit(value?: string): number | undefined {
  if (!value) return undefined;
  const parsed = Number(value);
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return undefined;
}

async function loadFeedProducts(xmlPath: string): Promise<FeedProduct[]> {
  const xmlContent = await readFile(xmlPath, 'utf-8');
  const parsed = await parseStringPromise(xmlContent, {
    explicitArray: false,
    mergeAttrs: true,
    trim: true,
  });

  const rawProducts = parsed?.products?.product;
  if (!rawProducts) {
    return [];
  }

  return Array.isArray(rawProducts) ? rawProducts : [rawProducts];
}

function getEnvVar(name: string, fallback?: string): string {
  const value = process.env[name];
  if (value && value.trim()) {
    return value.trim();
  }
  if (fallback !== undefined) {
    return fallback;
  }
  throw new Error(`Missing required environment variable: ${name}`);
}


function ensureAbsoluteUrl(baseUrl: string, candidate?: string): string | undefined {
  if (!candidate) return undefined;
  try {
    if (/^https?:\/\//i.test(candidate)) {
      return candidate;
    }
    const normalizedBase = baseUrl.endsWith('/')
      ? baseUrl
      : `${baseUrl}/`;
    const relative = candidate.startsWith('/')
      ? candidate.slice(1)
      : candidate;
    return new URL(relative, normalizedBase).toString();
  } catch (error) {
    console.warn(`Unable to build absolute URL from '${candidate}':`, error);
    return undefined;
  }
}

function slugify(value?: string): string {
  if (!value) return '';
  return value
    .normalize('NFKD')
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/[\s_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .toLowerCase();
}

function applyTemplate(template: string, product: FeedProduct): string {
  return template
    .replace(/{id}/gi, encodeURIComponent(product.id ?? ''))
    .replace(/{code}/gi, encodeURIComponent(product.code ?? ''))
    .replace(/{nameSlug}/gi, encodeURIComponent(slugify(product.name)));
}

function sanitizeDescription(description?: string): string {
  if (!description) return '';
  const decoded = he.decode(description);
  const withLineBreaks = decoded
    .replace(/<\s*br\s*\/?>/gi, '\n')
    .replace(/<\/\s*(p|div|h\d)\s*>/gi, '\n')
    .replace(/<\s*(p|div|h\d)[^>]*>/gi, '\n')
    .replace(/<\s*li[^>]*>\s*/gi, '\n• ')
    .replace(/<\/\s*li\s*>/gi, '')
    .replace(/<\/\s*(ul|ol)\s*>/gi, '\n');

  const withoutTags = withLineBreaks.replace(/<[^>]*>/g, ' ');

  return withoutTags
    .split('\n')
    .map((line) => line.replace(/\s+/g, ' ').trim())
    .filter((line) => line.length > 0)
    .join('\n');
}

function toArray<T>(value: T | T[] | undefined): T[] {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

interface MetaDataMaps {
  categories: Record<string, string>;
  manufacturers: Record<string, string>;
}

async function loadMetaDataMaps(metaPath: string): Promise<MetaDataMaps> {
  try {
    const xmlContent = await readFile(metaPath, 'utf-8');
    const parsed = await parseStringPromise(xmlContent, {
      explicitArray: false,
      mergeAttrs: true,
      trim: true,
    });

    const categoryMap: Record<string, string> = {};
    const manufacturerMap: Record<string, string> = {};

    const rootCategories = parsed?.meta?.categories?.category;
    if (rootCategories) {
      const walk = (node: MetaCategory, trail: string[]) => {
        if (!node) return;
        const name = (node.name || '').trim();
        const nextTrail = name ? [...trail, name] : trail;
        if (node.id) {
          const pathLabel = nextTrail.length ? nextTrail.join(' > ') : name || node.id;
          categoryMap[node.id] = pathLabel;
        }
        toArray(node.category as MetaCategory | MetaCategory[] | undefined).forEach((child) =>
          walk(child, nextTrail),
        );
      };

      toArray(rootCategories as MetaCategory | MetaCategory[]).forEach((category) =>
        walk(category, []),
      );
    }

    const manufacturerEntries = toArray(parsed?.meta?.manufacturers?.manufacturer as MetaManufacturer | MetaManufacturer[] | undefined);
    for (const entry of manufacturerEntries) {
      if (!entry?.id) continue;
      const name = (entry.name || '').trim();
      if (name) {
        manufacturerMap[entry.id] = name;
      }
    }

    return { categories: categoryMap, manufacturers: manufacturerMap };
  } catch (error) {
    console.warn(`Unable to load metadata from ${metaPath}:`, error instanceof Error ? error.message : error);
    return { categories: {}, manufacturers: {} };
  }
}

type SqliteDatabase = InstanceType<typeof DatabaseConstructor>;

let dbInstance: SqliteDatabase | null = null;

async function getDatabase(): Promise<SqliteDatabase> {
  if (dbInstance) {
    return dbInstance;
  }
  await mkdir(path.dirname(DB_PATH), { recursive: true });
  const instance = new DatabaseConstructor(DB_PATH);
  instance.pragma('journal_mode = WAL');
  instance.exec(`
    CREATE TABLE IF NOT EXISTS product_variants (
      offer_id TEXT PRIMARY KEY,
      item_group_id TEXT,
      hash TEXT NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `);
  dbInstance = instance;
  return instance;
}

function loadCachedVariants(db: SqliteDatabase): Map<string, CachedVariantRecord> {
  const stmt = db.prepare('SELECT offer_id AS offerId, item_group_id AS itemGroupId, hash, updated_at AS updatedAt FROM product_variants');
  const rows = stmt.all() as CachedVariantRecord[];
  const map = new Map<string, CachedVariantRecord>();
  for (const row of rows) {
    map.set(row.offerId, row);
  }
  return map;
}

function upsertVariantRecord(db: SqliteDatabase, record: CachedVariantRecord): void {
  const stmt = db.prepare(`
    INSERT INTO product_variants (offer_id, item_group_id, hash, updated_at)
    VALUES (@offerId, @itemGroupId, @hash, @updatedAt)
    ON CONFLICT(offer_id) DO UPDATE SET
      item_group_id=excluded.item_group_id,
      hash=excluded.hash,
      updated_at=excluded.updated_at
  `);
  stmt.run(record);
}

function deleteVariantRecords(db: SqliteDatabase, offerIds: string[]): void {
  if (!offerIds.length) return;
  const stmt = db.prepare('DELETE FROM product_variants WHERE offer_id = ?');
  for (const offerId of offerIds) {
    stmt.run(offerId);
  }
}

function hashProduct(product: SchemaProduct): string {
  const payload = {
    offerId: product.offerId,
    itemGroupId: product.itemGroupId,
    title: product.title,
    description: product.description,
    link: product.link,
    imageLink: product.imageLink,
    additionalImageLinks: product.additionalImageLinks,
    contentLanguage: product.contentLanguage,
    targetCountry: product.targetCountry,
    channel: product.channel,
    availability: product.availability,
    condition: product.condition,
    price: product.price,
    brand: product.brand,
    mpn: product.mpn,
    googleProductCategory: product.googleProductCategory,
    customLabel0: product.customLabel0,
    sizes: product.sizes,
    color: product.color,
    gender: product.gender,
    ageGroup: product.ageGroup,
    productTypes: product.productTypes,
  };
  return createHash('sha256').update(JSON.stringify(payload)).digest('hex');
}

function createRestProductId(offerId: string, mapping: MappingOptions): string {
  return `${mapping.channel}:${mapping.contentLanguage}:${mapping.targetCountry}:${offerId}`;
}

async function deleteStaleProducts(
  offerIds: string[],
  merchantId: string,
  mappingOptions: MappingOptions,
  contentApi: content_v2_1.Content,
  db: SqliteDatabase,
  dryRun: boolean,
): Promise<void> {
  if (!offerIds.length) {
    return;
  }

  for (let index = 0; index < offerIds.length; index += 1) {
    if (isStopRequested()) {
      console.log('Stop requested. Halting product deletion loop.');
      break;
    }
    const offerId = offerIds[index];
    const restId = createRestProductId(offerId, mappingOptions);
    if (dryRun) {
      console.log(`[dry-run] (${index + 1}/${offerIds.length}) Would delete product ${restId}`);
      continue;
    }

    try {
      await contentApi.products.delete({
        merchantId,
        productId: restId,
      });
      deleteVariantRecords(db, [offerId]);
      console.log(`Deleted product ${restId} (${index + 1}/${offerIds.length})`);
    } catch (error) {
      console.error(`Failed to delete product ${restId}:`, error);
    }
  }
}

async function loadInlineCredentialsFromEnv(): Promise<JWTInput | undefined> {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) {
    return undefined;
  }

  const trimmed = raw.trim();
  if (!trimmed) {
    return undefined;
  }

  try {
    if (trimmed.startsWith('{')) {
      return JSON.parse(trimmed) as JWTInput;
    }
    const candidatePath = path.isAbsolute(trimmed)
      ? trimmed
      : path.resolve(process.cwd(), trimmed);
    const fileContent = await readFile(candidatePath, 'utf-8');
    return JSON.parse(fileContent) as JWTInput;
  } catch (error) {
    console.warn(
      'Unable to parse GOOGLE_SERVICE_ACCOUNT_JSON. Provide valid JSON or a path to the JSON file.',
      error instanceof Error ? error.message : error,
    );
    return undefined;
  }
}

const EXCLUDED_SIZE_TOKENS = new Set(['3XL', '4XL', '5XL', '6XL']);

function shouldExcludeSize(input: string): boolean {
  const trimmed = input.trim().toUpperCase();
  if (!trimmed) {
    return false;
  }
  return EXCLUDED_SIZE_TOKENS.has(trimmed);
}

function extractProductSizes(product: FeedProduct): {
  values: string[],
  hadInput: boolean,
  sourceMap: Map<string, FeedSize>,
} {
  const sizeEntries = toArray(product.sizes?.size as FeedSize | FeedSize[] | undefined);
  const normalizedSizes: string[] = [];
  const seen = new Set<string>();
  const sourceMap = new Map<string, FeedSize>();

  for (const entry of sizeEntries) {
    if (!entry) continue;
    const candidate = entry.value || entry.name || entry.size_label_1;
    if (!candidate) continue;
    const normalized = String(candidate).trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    if (shouldExcludeSize(normalized)) {
      continue;
    }
    seen.add(normalized);
    normalizedSizes.push(normalized);
    sourceMap.set(normalized.toLowerCase(), entry);
  }

  return {
    values: normalizedSizes,
    hadInput: sizeEntries.length > 0,
    sourceMap,
  };
}

function extractProductColors(product: FeedProduct): string[] {
  const colorEntries = toArray(product.colors?.color as FeedColor | FeedColor[] | undefined);
  const normalizedColors: string[] = [];
  const seen = new Set<string>();

  for (const entry of colorEntries) {
    if (!entry) continue;
    const colorCandidates = [
      entry.name,
      ...(toArray(entry.clr as FeedClr | FeedClr[] | undefined).map((clr) => clr?.name)),
    ];
    for (const candidate of colorCandidates) {
      if (!candidate) continue;
      const normalized = String(candidate).trim();
      if (!normalized || seen.has(normalized)) {
        continue;
      }
      seen.add(normalized);
      normalizedColors.push(normalized);
    }
  }

  return normalizedColors;
}

function getFeedColorEntries(product: FeedProduct): FeedColor[] {
  return toArray(product.colors?.color as FeedColor | FeedColor[] | undefined);
}

function extractViewImageEntries(product: FeedProduct): FeedImage[] {
  const views = toArray(product.views?.view as FeedView | FeedView[] | undefined);
  if (!views.length) {
    return [];
  }
  const priorityNames = [
    'front',
    'back',
    'right sleeve',
    'right sleeve (short)',
    'right sleeve (long)',
    'left sleeve',
    'left sleeve (short)',
    'left sleeve (long)',
  ];
  const priorityMap = new Map<string, number>();
  priorityNames.forEach((name, index) => priorityMap.set(name, index));
  return views
    .map((view, index) => {
      const normalizedName = (view.name ?? '').toLowerCase();
      const priority = priorityMap.get(normalizedName) ?? (priorityNames.length + index);
      return {
        type: view.name ? `view:${view.name}` : 'view',
        src: view.image_url,
        order: priority,
      };
    })
    .filter((entry) => Boolean(entry.src))
    .sort((a, b) => a.order - b.order)
    .map(({ type, src }) => ({ type, src }));
}

function getImageCategory(entry: FeedImage): ImageCategory {
  const type = (entry.type ?? '').toLowerCase();
  if (type.includes('front')) {
    return 'front';
  }
  if (type.includes('back')) {
    return 'back';
  }
  return 'other';
}

function getColorNameTokens(entry: FeedColor): string[] {
  const rawNames = [
    entry.name,
    ...toArray(entry.clr as FeedClr | FeedClr[] | undefined).map((clr) => clr?.name),
  ];
  return rawNames
    .filter((name): name is string => Boolean(name))
    .map((name) => name.toLowerCase());
}

function getDefaultColorEntry(product: FeedProduct): FeedColor | undefined {
  const entries = getFeedColorEntries(product);
  if (!entries.length) {
    return undefined;
  }
  if (product.default_color_id) {
    const defaultId = String(product.default_color_id);
    const match = entries.find((entry) =>
      String(entry.id) === defaultId || String(entry.color_id) === defaultId,
    );
    if (match) {
      return match;
    }
  }
  return entries[0];
}

function resolveColorEntriesForLabel(
  label: string | undefined,
  product: FeedProduct,
  entries: FeedColor[],
): FeedColor[] {
  if (!entries.length) {
    return [];
  }
  if (!label) {
    const defaultEntry = getDefaultColorEntry(product);
    return defaultEntry ? [defaultEntry] : [entries[0]];
  }
  if (label === 'Multicolor') {
    return entries;
  }
  const tokens = label
    .split('/')
    .map((token) => token.trim().toLowerCase())
    .filter(Boolean);
  if (!tokens.length) {
    const defaultEntry = getDefaultColorEntry(product);
    return defaultEntry ? [defaultEntry] : [entries[0]];
  }
  const result: FeedColor[] = [];
  const used = new Set<FeedColor>();
  for (const token of tokens) {
    const match = entries.find((entry) => {
      if (used.has(entry)) return false;
      return getColorNameTokens(entry).some((name) =>
        name.includes(token) || token.includes(name),
      );
    });
    if (match) {
      used.add(match);
      result.push(match);
    }
  }
  if (!result.length) {
    const defaultEntry = getDefaultColorEntry(product);
    if (defaultEntry) {
      result.push(defaultEntry);
    }
  }
  return result.length ? result : [entries[0]];
}

function buildVariantOfferId(baseOfferId: string, color?: string): string {
  const normalize = (value: string) => value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  const suffix = color ? normalize(color) : 'variant';
  return `${baseOfferId}-${suffix}`;
}

function buildTokenSet(sources: Array<string | undefined>): Set<string> {
  const normalized = sources
    .map((part) => part?.toLowerCase() ?? '')
    .filter(Boolean)
    .join(' ')
    .replace(/['’]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();

  if (!normalized) {
    return new Set();
  }

  return new Set(
    normalized
      .split(' ')
      .map((token) => token.trim())
      .filter(Boolean),
  );
}

const FEMALE_KEYWORDS = ['women', 'womens', 'woman', "woman's", 'lady', 'ladies', 'female', 'girl', 'girls', "girls'", "girl's"];
const MALE_KEYWORDS = ['men', 'mens', 'man', "man’s", 'male', 'boy', 'boys', "boys'", "boy's", 'guy', 'guys'];
const UNISEX_KEYWORDS = ['unisex'];
const AGE_KEYWORDS: Record<Exclude<AgeGroupValue, 'adult'>, string[]> = {
  newborn: ['newborn', 'new-born'],
  infant: ['infant', 'baby', 'layette'],
  toddler: ['toddler'],
  kids: ['youth', 'kid', 'kids', 'child', 'children', 'teen', 'junior', 'boys', 'girls'],
};

interface ColorKeyword {
  value: string;
  label: string;
}

const NEUTRAL_COLOR_KEYWORDS: ColorKeyword[] = [
  { value: 'black', label: 'Black' },
  { value: 'white', label: 'White' },
  { value: 'grey', label: 'Grey' },
  { value: 'gray', label: 'Grey' },
];
const BLUE_COLOR_KEYWORDS: ColorKeyword[] = [
  { value: 'navy', label: 'Navy' },
  { value: 'blue', label: 'Blue' },
  { value: 'royal', label: 'Royal' },
];

function matchColorKeyword(
  rawValue: string,
  normalizedValue: string,
  keywords: ColorKeyword[],
): string | undefined {
  for (const keyword of keywords) {
    if (normalizedValue === keyword.value) {
      return keyword.label;
    }
  }
  for (const keyword of keywords) {
    if (normalizedValue.includes(keyword.value)) {
      return keyword.label;
    }
  }
  return undefined;
}

function collectColorMatches(
  colors: string[],
  keywords: ColorKeyword[],
  limit = 3,
): Set<string> {
  const matches = new Set<string>();
  for (const keyword of keywords) {
    if (matches.size >= limit) break;

    let exactMatch: string | undefined;
    let fuzzyMatch: string | undefined;

    for (const raw of colors) {
      const normalized = raw.trim().toLowerCase();
      if (!normalized) continue;
      if (normalized === keyword.value) {
        exactMatch = keyword.label;
        break;
      }
      if (!fuzzyMatch && normalized.includes(keyword.value)) {
        fuzzyMatch = keyword.label;
      }
    }

    if (exactMatch) {
      matches.add(exactMatch);
    } else if (fuzzyMatch) {
      matches.add(fuzzyMatch);
    }
  }
  return matches;
}

function buildColorGroups(colors: string[]): string[] {
  if (!colors.length) return [];

  const neutralSet = collectColorMatches(colors, NEUTRAL_COLOR_KEYWORDS);
  const blueSet = collectColorMatches(colors, BLUE_COLOR_KEYWORDS);
  const normalizeColor = (value: string) => {
    const trimmed = value.trim().toLowerCase();
    return trimmed || undefined;
  };
  const totalColors = new Set<string>();
  const matchedColors = new Set<string>();
  for (const raw of colors) {
    const normalized = normalizeColor(raw);
    if (normalized) {
      totalColors.add(normalized);
    }
  }
  neutralSet.forEach((color) => {
    const normalized = normalizeColor(color);
    if (normalized) matchedColors.add(normalized);
  });
  blueSet.forEach((color) => {
    const normalized = normalizeColor(color);
    if (normalized) matchedColors.add(normalized);
  });
  const hasOther = totalColors.size > matchedColors.size;

  const groups: string[] = [];
  const totalPrimaryColors = neutralSet.size + blueSet.size;
  if (neutralSet.size && blueSet.size && totalPrimaryColors <= 3) {
    groups.push([...neutralSet, ...blueSet].join('/'));
  } else {
    if (neutralSet.size) {
      groups.push(Array.from(neutralSet).join('/'));
    }
    if (blueSet.size) {
      groups.push(Array.from(blueSet).join('/'));
    }
  }
  if (hasOther) {
    groups.push('Multicolor');
  }

  return groups;
}

function inferGenderFromProduct(
  product: FeedProduct,
  productTypes: string[],
  options: InferenceOptions,
): GenderValue | undefined {
  const tokenSet = buildTokenSet([product.name, product.code, ...productTypes]);
  if (options.includeDescription) {
    const descriptionTokens = buildTokenSet([product.description ? he.decode(product.description) : undefined]);
    descriptionTokens.forEach((token) => tokenSet.add(token));
  }
  if (!tokenSet.size) return DEFAULT_GENDER;

  if (UNISEX_KEYWORDS.some((keyword) => tokenSet.has(keyword))) {
    return 'unisex';
  }

  const hasFemale = FEMALE_KEYWORDS.some((keyword) => tokenSet.has(keyword));
  const hasMale = MALE_KEYWORDS.some((keyword) => tokenSet.has(keyword));

  if (hasFemale && hasMale) return 'unisex';
  if (hasFemale) return 'female';
  if (hasMale) return 'male';
  return DEFAULT_GENDER;
}

function inferAgeGroupFromProduct(
  product: FeedProduct,
  productTypes: string[],
  options: InferenceOptions,
): AgeGroupValue | undefined {
  const tokenSet = buildTokenSet([product.name, product.code, ...productTypes]);
  if (options.includeDescription) {
    const descriptionTokens = buildTokenSet([product.description ? he.decode(product.description) : undefined]);
    descriptionTokens.forEach((token) => tokenSet.add(token));
  }
  if (!tokenSet.size) return DEFAULT_AGE_GROUP;

  if (AGE_KEYWORDS.newborn.some((keyword) => tokenSet.has(keyword))) return 'newborn';
  if (AGE_KEYWORDS.infant.some((keyword) => tokenSet.has(keyword))) return 'infant';
  if (AGE_KEYWORDS.toddler.some((keyword) => tokenSet.has(keyword))) return 'toddler';
  if (AGE_KEYWORDS.kids.some((keyword) => tokenSet.has(keyword))) return 'kids';
  if (tokenSet.has('adult')) return 'adult';
  return DEFAULT_AGE_GROUP;
}

function extractProductCategories(
  product: FeedProduct,
  categoryMap: Record<string, string>,
): string[] {
  const categoryEntries = toArray(
    product.categories?.category as FeedProductCategory | FeedProductCategory[] | undefined,
  );
  const values: string[] = [];
  const seen = new Set<string>();

  for (const entry of categoryEntries) {
    if (!entry) continue;
    const id = entry.category_id || entry.id;
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const mapped = categoryMap[id];
    values.push(mapped || id);
  }

  return values;
}

function getDefaultColorId(product: FeedProduct): string | undefined {
  if (product.default_color_id) {
    return String(product.default_color_id);
  }
  const colors = toArray(product.colors?.color as FeedColor | FeedColor[] | undefined);
  return colors[0]?.id || colors[0]?.color_id;
}

function getDefaultSize(product: FeedProduct): FeedSize | undefined {
  const sizes = toArray(product.sizes?.size as FeedSize | FeedSize[] | undefined);
  if (!sizes.length) return undefined;
  const selected = sizes.find((size) =>
    typeof size.selected === 'string' && size.selected.toLowerCase() === 'true',
  );
  return selected || sizes[0];
}

function getPreferredImageSizeEntry(): FeedSize {
  return { id: PREFERRED_IMAGE_SIZE_ID };
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function replacePlaceholder(
  input: string,
  placeholder: string,
  value?: string,
): string {
  if (!input || !value) return input;
  const rawRegex = new RegExp(`\\[${placeholder}\\]`, 'gi');
  const encodedPlaceholder = encodeURIComponent(`[${placeholder}]`);
  const encodedRegex = new RegExp(escapeRegex(encodedPlaceholder), 'gi');
  return input
    .replace(rawRegex, value)
    .replace(encodedRegex, encodeURIComponent(value));
}

function materializeImageSource(
  src: string | undefined,
  product: FeedProduct,
  color?: FeedColor,
  size?: FeedSize,
): string | undefined {
  if (!src) return undefined;
  const defaultColorId = getDefaultColorId(product);
  const defaultSize = getDefaultSize(product);
  const colorId = color?.color_id || color?.id || defaultColorId;
  const sizeEntry = size ?? defaultSize;
  let result = src;

  if (colorId) {
    result = replacePlaceholder(result, 'COLOR_ID', String(colorId));
  }
  if (sizeEntry?.id) {
    result = replacePlaceholder(result, 'SIZE_ID', String(sizeEntry.id));
  }
  const sizeToken = sizeEntry?.id || sizeEntry?.value || sizeEntry?.name || sizeEntry?.size_label_1;
  if (sizeToken) {
    result = replacePlaceholder(result, 'SIZE', sizeToken);
  }

  return result;
}

function buildVariantImageLinks(
  imageEntries: FeedImage[],
  product: FeedProduct,
  colorEntries: FeedColor[],
  sizeEntry: FeedSize | undefined,
  assetBaseUrl: string,
): string[] {
  const urls: string[] = [];
  const seen = new Set<string>();
  const colorsToApply = colorEntries.length ? colorEntries : [undefined];
  const preferredImageSize = getPreferredImageSizeEntry();
  const effectiveSizeEntry = preferredImageSize ?? sizeEntry;

  const tryAddImage = (entry: FeedImage, colorEntry: FeedColor | undefined) => {
    const materialized = materializeImageSource(entry?.src, product, colorEntry, effectiveSizeEntry);
    const absolute = ensureAbsoluteUrl(assetBaseUrl, materialized);
    if (absolute && !seen.has(absolute)) {
      seen.add(absolute);
      urls.push(absolute);
      return true;
    }
    return false;
  };

  const addImagesForCategory = (
    category: ImageCategory,
    predicate: (entry: FeedImage) => boolean,
  ) => {
    for (const entry of imageEntries) {
      if (!predicate(entry)) continue;
      for (const colorEntry of colorsToApply) {
        if (tryAddImage(entry, colorEntry) && urls.length >= MAX_VARIANT_IMAGE_COUNT) {
          return;
        }
      }
      if (urls.length >= MAX_VARIANT_IMAGE_COUNT) {
        return;
      }
    }
  };

  const imagesByCategory = {
    'front': (entry: FeedImage) => getImageCategory(entry) === 'front',
    'back': (entry: FeedImage) => getImageCategory(entry) === 'back',
    'other': (entry: FeedImage) => getImageCategory(entry) === 'other',
  } as const;

  addImagesForCategory('front', imagesByCategory.front);
  if (urls.length < MAX_VARIANT_IMAGE_COUNT) {
    addImagesForCategory('back', imagesByCategory.back);
  }
  if (urls.length < MAX_VARIANT_IMAGE_COUNT) {
    addImagesForCategory('other', imagesByCategory.other);
  }

  if (urls.length < MAX_VARIANT_IMAGE_COUNT) {
    for (const entry of imageEntries) {
      for (const colorEntry of colorsToApply) {
        if (tryAddImage(entry, colorEntry) && urls.length >= MAX_VARIANT_IMAGE_COUNT) {
          return urls;
        }
      }
      if (urls.length >= MAX_VARIANT_IMAGE_COUNT) {
        break;
      }
    }
  }

  return urls;
}

function mapFeedProductToGoogleProduct(
  product: FeedProduct,
  options: MappingOptions,
  categoryMap: Record<string, string>,
  manufacturerMap: Record<string, string>,
  inferenceOptions: InferenceOptions,
): SchemaProduct[] {
  const priceValue = Number(product.price ?? product.cheapest_price ?? 0);
  const templatePath = applyTemplate(options.productPathTemplate, product);
  const productLink = ensureAbsoluteUrl(options.baseStoreUrl, templatePath) ?? options.baseStoreUrl;

  const baseImageEntries = toArray(product.images?.image as FeedImage | FeedImage[] | undefined);
  const viewImageEntries = extractViewImageEntries(product);
  const imageEntries = [...viewImageEntries, ...baseImageEntries];
  const defaultImageLinks = buildVariantImageLinks(
    imageEntries,
    product,
    [],
    undefined,
    options.assetBaseUrl,
  );
  const [fallbackPrimaryImage, ...fallbackAdditionalImages] = defaultImageLinks;

  const sanitizedDescription = sanitizeDescription(product.description || String(product.name || ''));
  const productIdPart = product.id ? String(product.id) : undefined;
  const codePart = product.code ? String(product.code) : undefined;
  const baseOfferId = codePart && productIdPart
    ? `${codePart}-${productIdPart}`
    : (productIdPart ?? codePart ?? slugify(product.name)) ?? `product-${Date.now()}`;
  const itemGroupId = codePart && productIdPart
    ? `${codePart}-${productIdPart}`
    : (codePart ?? productIdPart);
  const {
    values: sizes,
    hadInput: hadSizeEntries,
    sourceMap: sizeSourceMap,
  } = extractProductSizes(product);
  if (hadSizeEntries && !sizes.length) {
    return [];
  }
  const colors = extractProductColors(product);
  const feedColorEntries = getFeedColorEntries(product);
  const productTypes = extractProductCategories(product, categoryMap);
  const colorGroups = buildColorGroups(colors);
  const sizeValues = sizes.length ? sizes : [undefined];
  const colorValues = colorGroups.length ? colorGroups : [undefined];
  const genderInference = inferGenderFromProduct(product, productTypes, inferenceOptions) ?? DEFAULT_GENDER;
  const ageGroup = inferAgeGroupFromProduct(product, productTypes, inferenceOptions) ?? DEFAULT_AGE_GROUP;
  const gender = ageGroup === 'kids' ? DEFAULT_KIDS_GENDER : genderInference;
  const manufacturerName = product.manufacturer_id
    ? manufacturerMap[String(product.manufacturer_id)] ?? String(product.manufacturer_id)
    : undefined;

  const baseProduct = {
    itemGroupId: itemGroupId ?? baseOfferId,
    title: product.name?.trim() || baseOfferId,
    description: sanitizedDescription,
    link: productLink,
    contentLanguage: options.contentLanguage,
    targetCountry: options.targetCountry,
    channel: options.channel,
    availability: options.defaultAvailability,
    condition: options.defaultCondition,
    price: priceValue
      ? {
          currency: options.priceCurrency,
          value: priceValue.toFixed(2),
        }
      : undefined,
    brand: manufacturerName,
    mpn: product.code,
    googleProductCategory: options.defaultGpc,
    customLabel0: product.type_id ? String(product.type_id) : undefined,
    gender,
    ageGroup,
    productTypes: productTypes.length ? productTypes : undefined,
  };

  const variants: SchemaProduct[] = [];
  for (const colorValue of colorValues) {
    for (const sizeValue of sizeValues) {
      const colorEntries = resolveColorEntriesForLabel(colorValue, product, feedColorEntries);
      const sizeEntry = sizeValue ? sizeSourceMap.get(sizeValue.toLowerCase()) : undefined;
      const variantImageLinks = buildVariantImageLinks(
        imageEntries,
        product,
        colorEntries,
        sizeEntry,
        options.assetBaseUrl,
      );
      const [variantPrimaryImage, ...variantAdditionalImages] = variantImageLinks.length
        ? variantImageLinks
        : defaultImageLinks;

      const variant: SchemaProduct = {
        ...baseProduct,
        offerId: buildVariantOfferId(baseOfferId, colorValue) + (sizeValue ? `-${sizeValue.toLowerCase()}` : ''),
        itemGroupId: baseProduct.itemGroupId ?? baseOfferId,
        color: colorValue,
        sizes: sizeValue ? [sizeValue] : undefined,
        imageLink: variantPrimaryImage ?? fallbackPrimaryImage,
        additionalImageLinks: variantAdditionalImages.length
          ? variantAdditionalImages
          : (fallbackAdditionalImages.length ? fallbackAdditionalImages : undefined),
      };
      variant.gender = gender;
      variant.ageGroup = ageGroup;
      variants.push(variant);
    }
  }

  return variants.length ? variants : [{ ...baseProduct, offerId: baseOfferId }];
}

async function createContentClient() {
  const credentialsPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  const inlineCredentials = await loadInlineCredentialsFromEnv();

  const googleAuth = new google.auth.GoogleAuth({
    keyFile: credentialsPath,
    credentials: inlineCredentials,
    scopes: ['https://www.googleapis.com/auth/content'],
  });

  return google.content({ version: 'v2.1', auth: googleAuth });
}

async function uploadProducts(
  items: UploadQueueItem[],
  merchantId: string,
  contentApi: content_v2_1.Content,
  db: SqliteDatabase,
  dryRun: boolean,
) {
  if (!items.length) {
    console.log('No product updates detected.');
    return;
  }

  const report = {
    success: 0,
    failed: 0,
  };

  for (let index = 0; index < items.length; index += 1) {
    if (isStopRequested()) {
      console.log('Stop requested. Halting product upload loop.');
      break;
    }
    const { product, hash } = items[index];
    const displayColor = product.color ?? 'n/a';
    const displaySize = product.sizes?.[0] ?? 'n/a';
    const variantLabel = `color: ${displayColor}, size: ${displaySize}, gender: ${product.gender ?? 'n/a'}, ageGroup: ${product.ageGroup ?? 'n/a'}`;

    if (!product.offerId) {
      console.warn('Skipping product with missing offerId.');
      report.failed += 1;
      continue;
    }

    if (dryRun) {
      console.log(`[dry-run] (${index + 1}/${items.length}) Would upload product ${product.offerId} (${variantLabel})`);
      report.success += 1;
      continue;
    }

    try {
      await contentApi.products.insert({
        merchantId,
        requestBody: product,
      });
      console.log(`Uploaded product ${product.offerId} (${index + 1}/${items.length}) (${variantLabel})`);
      report.success += 1;
      upsertVariantRecord(db, {
        offerId: product.offerId,
        itemGroupId: product.itemGroupId ?? undefined,
        hash,
        updatedAt: Date.now(),
      });
    } catch (error) {
      console.error(`Failed to upload product ${product.offerId}:`, error);
      report.failed += 1;
    }

    // break;
  }

  console.log(
    `Upload finished. Success: ${report.success}, Failed: ${report.failed}`,
  );
}

async function main() {
  const startTime = Date.now();
  const formatTimestamp = (timestamp: number) =>
    new Date(timestamp).toLocaleString('en-US', { timeZone: 'America/New_York' });
  console.log(`[run] Started at ${formatTimestamp(startTime)} (EST)`);

  const cliOptions = parseCliArgs(process.argv.slice(2));
  const merchantId = getEnvVar('GOOGLE_MERCHANT_ID');
  const storeBaseUrl = getEnvVar('STORE_BASE_URL', 'https://l1print.com/');

  const { categories: categoryMap, manufacturers: manufacturerMap } = await loadMetaDataMaps(cliOptions.metaPath);
  if (Object.keys(categoryMap).length) {
    console.log(`Loaded ${Object.keys(categoryMap).length} categories from ${cliOptions.metaPath}`);
  } else {
    console.warn(`No categories parsed from ${cliOptions.metaPath}. Product types will use raw IDs.`);
  }
  if (!Object.keys(manufacturerMap).length) {
    console.warn(`No manufacturers parsed from ${cliOptions.metaPath}. Brand names will use raw IDs.`);
  }

  const mappingOptions: MappingOptions = {
    baseStoreUrl: storeBaseUrl,
    assetBaseUrl: process.env.STORE_ASSET_BASE_URL ?? storeBaseUrl,
    productPathTemplate: process.env.PRODUCT_PATH_TEMPLATE ?? '/blank_product/{id}/{nameSlug}',
    contentLanguage: process.env.GOOGLE_CONTENT_LANGUAGE ?? 'en',
    targetCountry: process.env.GOOGLE_TARGET_COUNTRY ?? 'US',
    channel: (process.env.GOOGLE_CHANNEL ?? 'online') as 'online' | 'local',
    priceCurrency: process.env.GOOGLE_PRICE_CURRENCY ?? 'USD',
    defaultAvailability: process.env.GOOGLE_DEFAULT_AVAILABILITY ?? 'in stock',
    defaultCondition: process.env.GOOGLE_PRODUCT_CONDITION ?? 'new',
    defaultGpc: process.env.GOOGLE_DEFAULT_PRODUCT_CATEGORY,
  };

  console.log(`Loading feed from: ${cliOptions.xmlPath}`);
  const feedProducts = await loadFeedProducts(cliOptions.xmlPath);
  if (!feedProducts.length) {
    console.warn('No products found in the feed. Proceeding to check for stale products to delete.');
  }

  const selectedProducts = typeof cliOptions.limit === 'number'
    ? feedProducts.slice(0, cliOptions.limit)
    : feedProducts;

  const inferenceOptions: InferenceOptions = {
    includeDescription: cliOptions.includeDescriptionInInference,
  };

  const googleProducts = selectedProducts.flatMap((item) =>
    mapFeedProductToGoogleProduct(item, mappingOptions, categoryMap, manufacturerMap, inferenceOptions),
  );
  console.log(`Prepared ${googleProducts.length} variant products for evaluation.`);

  const db = await getDatabase();
  const cachedVariants = loadCachedVariants(db);
  const seenOfferIds = new Set<string>();
  const uploadQueue: UploadQueueItem[] = [];

  for (const product of googleProducts) {
    if (!product.offerId) continue;
    seenOfferIds.add(product.offerId);
    const hash = hashProduct(product);
    const cached = cachedVariants.get(product.offerId);
    if (cached && cached.hash === hash) {
      continue;
    }
    uploadQueue.push({ product, hash });
  }

  const staleOfferIds = Array.from(cachedVariants.keys()).filter((offerId) => !seenOfferIds.has(offerId));

  if (!uploadQueue.length) {
    console.log('No new or updated products detected in the current feed.');
  } else {
    console.log(`Detected ${uploadQueue.length} products that need to be created or updated.`);
  }
  if (staleOfferIds.length) {
    console.log(`Detected ${staleOfferIds.length} products that need to be deleted.`);
  }

  const contentApi = await createContentClient();

  if (staleOfferIds.length) {
    await deleteStaleProducts(staleOfferIds, merchantId, mappingOptions, contentApi, db, cliOptions.dryRun);
  }

  await uploadProducts(uploadQueue, merchantId, contentApi, db, cliOptions.dryRun);

  const endTime = Date.now();
  const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
  console.log(`[run] Finished at ${formatTimestamp(endTime)} (EST, duration: ${durationSeconds}s)`);
}

main().catch((error) => {
  console.error('Fatal error while syncing Google Merchant Center products:', error);
  process.exit(1);
});

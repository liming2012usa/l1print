import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
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

const DEFAULT_XML_PATH = path.resolve(__dirname, 'data_feeds', 'products.xml');
const DEFAULT_META_PATH = path.resolve(__dirname, 'data_feeds', 'meta_data.xml');
const COLOR_ATTRIBUTE_MAX_LENGTH = 100;
const DEFAULT_GENDER: GenderValue = 'unisex';
const DEFAULT_AGE_GROUP: AgeGroupValue = 'adult';

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
    } else if (arg === '--limit' && args[i + 1]) {
      limit = parseLimit(args[i + 1]);
      i += 1;
    } else if (arg.startsWith('--limit=')) {
      const [, value] = arg.split('=');
      limit = parseLimit(value);
    }
  }

  return { xmlPath, metaPath, dryRun, limit };
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

function extractProductSizes(product: FeedProduct): string[] {
  const sizeEntries = toArray(product.sizes?.size as FeedSize | FeedSize[] | undefined);
  const normalizedSizes: string[] = [];
  const seen = new Set<string>();

  for (const entry of sizeEntries) {
    if (!entry) continue;
    const candidate = entry.value || entry.name || entry.size_label_1;
    if (!candidate) continue;
    const normalized = String(candidate).trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    normalizedSizes.push(normalized);
  }

  return normalizedSizes;
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

function buildColorAttribute(colors: string[]): string | undefined {
  const sanitized = colors
    .map((color) => color.trim())
    .filter((color) => Boolean(color));
  if (!sanitized.length) {
    return undefined;
  }

  let result = '';
  for (const color of sanitized) {
    const candidate = result ? `${result},${color}` : color;
    if (candidate.length <= COLOR_ATTRIBUTE_MAX_LENGTH) {
      result = candidate;
      continue;
    }
    if (!result) {
      return color.slice(0, COLOR_ATTRIBUTE_MAX_LENGTH);
    }
    break;
  }

  if (!result) {
    return undefined;
  }
  return result;
}

function buildVariantOfferId(baseOfferId: string, size: string): string {
  const sanitizedSize = size
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'variant';
  return `${baseOfferId}-${sanitizedSize}`;
}

function buildProductSearchText(product: FeedProduct, productTypes: string[]): string {
  const parts: Array<string | undefined> = [
    product.name,
    product.description ? he.decode(product.description) : undefined,
    product.code,
    ...productTypes,
  ];
  return parts
    .map((part) => part?.toLowerCase() ?? '')
    .filter((part) => Boolean(part))
    .join(' ');
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

function inferGenderFromProduct(product: FeedProduct, productTypes: string[]): GenderValue | undefined {
  const text = buildProductSearchText(product, productTypes);
  if (!text) return undefined;

  const normalized = [
    (product.name ?? ''),
    product.description ? he.decode(product.description) : '',
    ...productTypes,
  ]
    .join(' ')
    .toLowerCase()
    .replace(/['’]/g, '')
    .replace(/[^a-z0-9]+/g, ' ');
  const tokens = normalized
    .split(' ')
    .map((token) => token.trim())
    .filter((token) => Boolean(token));
  const tokenSet = new Set(tokens);

  if (UNISEX_KEYWORDS.some((keyword) => tokenSet.has(keyword))) {
    return 'unisex';
  }

  const hasFemale = FEMALE_KEYWORDS.some((keyword) => tokenSet.has(keyword));
  const hasMale = MALE_KEYWORDS.some((keyword) => tokenSet.has(keyword));

  if (!hasFemale && /women/i.test(product.name ?? '')) {
    console.log('Gender inference fallback for product', product.id || product.code, 'text sample:', normalized.slice(0, 120));
  }

  if (hasFemale && hasMale) return 'unisex';
  if (hasFemale) return 'female';
  if (hasMale) return 'male';
  return 'male';
}

function inferAgeGroupFromProduct(product: FeedProduct, productTypes: string[]): AgeGroupValue | undefined {
  const text = buildProductSearchText(product, productTypes);
  if (!text) return undefined;

  if (AGE_KEYWORDS.newborn.some((keyword) => text.includes(keyword))) return 'newborn';
  if (AGE_KEYWORDS.infant.some((keyword) => text.includes(keyword))) return 'infant';
  if (AGE_KEYWORDS.toddler.some((keyword) => text.includes(keyword))) return 'toddler';
  if (AGE_KEYWORDS.kids.some((keyword) => text.includes(keyword))) return 'kids';
  if (text.includes('adult')) return 'adult';
  return undefined;
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

function materializeImageSource(src: string | undefined, product: FeedProduct): string | undefined {
  if (!src) return undefined;
  const defaultColorId = getDefaultColorId(product);
  const defaultSize = getDefaultSize(product);
  let result = src;

  if (defaultColorId) {
    result = replacePlaceholder(result, 'COLOR_ID', defaultColorId);
  }
  if (defaultSize?.id) {
    result = replacePlaceholder(result, 'SIZE_ID', String(defaultSize.id));
  }
  const sizeToken = defaultSize?.value || defaultSize?.name || defaultSize?.size_label_1;
  if (sizeToken) {
    result = replacePlaceholder(result, 'SIZE', sizeToken);
  }

  return result;
}

function mapFeedProductToGoogleProduct(
  product: FeedProduct,
  options: MappingOptions,
  categoryMap: Record<string, string>,
  manufacturerMap: Record<string, string>,
): SchemaProduct[] {
  const priceValue = Number(product.price ?? product.cheapest_price ?? 0);
  const templatePath = applyTemplate(options.productPathTemplate, product);
  const productLink = ensureAbsoluteUrl(options.baseStoreUrl, templatePath) ?? options.baseStoreUrl;

  const imageEntries = toArray(product.images?.image);
  const imageLinks = imageEntries
    .map((entry) => materializeImageSource(entry?.src, product))
    .map((entry) => ensureAbsoluteUrl(options.assetBaseUrl, entry))
    .filter((url): url is string => Boolean(url));
  const [primaryImage, ...additionalImages] = imageLinks;

  const sanitizedDescription = sanitizeDescription(product.description || String(product.name || ''));
  const baseOfferId = product.code || product.id || slugify(product.name) || `product-${Date.now()}`;
  const sizes = extractProductSizes(product);
  const colors = extractProductColors(product);
  const productTypes = extractProductCategories(product, categoryMap);
  const gender = inferGenderFromProduct(product, productTypes) ?? DEFAULT_GENDER;
  const ageGroup = inferAgeGroupFromProduct(product, productTypes) ?? DEFAULT_AGE_GROUP;
  const manufacturerName = product.manufacturer_id
    ? manufacturerMap[String(product.manufacturer_id)] ?? String(product.manufacturer_id)
    : undefined;

  const baseProduct = {
    itemGroupId: product.code || product.id,
    title: product.name?.trim() || baseOfferId,
    description: sanitizedDescription,
    link: productLink,
    imageLink: primaryImage,
    additionalImageLinks: additionalImages.length ? additionalImages : undefined,
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
    color: buildColorAttribute(colors),
    gender,
    ageGroup,
    productTypes: productTypes.length ? productTypes : undefined,
  };

  if (!sizes.length) {
    return [
      {
        ...baseProduct,
        offerId: baseOfferId,
      },
    ];
  }

  return sizes.map((size) => {
    const variant: SchemaProduct = {
      ...baseProduct,
      offerId: buildVariantOfferId(baseOfferId, size),
      itemGroupId: baseProduct.itemGroupId ?? baseOfferId,
      sizes: [size],
    };
    variant.gender = gender;
    variant.ageGroup = ageGroup;
    return variant;
  });
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
  products: SchemaProduct[],
  merchantId: string,
  contentApi: content_v2_1.Content,
  dryRun: boolean,
) {
  const report = {
    success: 0,
    failed: 0,
  };

  for (const product of products) {
    if (!product.offerId) {
      console.warn('Skipping product with missing offerId.');
      report.failed += 1;
      continue;
    }

    if (dryRun) {
      console.log(`[dry-run] Would upload product ${product.offerId}`);
      report.success += 1;
      continue;
    }

    try {
      await contentApi.products.insert({
        merchantId,
        requestBody: product,
      });
      console.log(`Uploaded product ${product.offerId} (size: ${product.sizes?.[0] ?? 'n/a'}, gender: ${product.gender ?? 'n/a'}, ageGroup: ${product.ageGroup ?? 'n/a'})`);
      report.success += 1;
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
    console.warn('No products found in the feed.');
    return;
  }

  const selectedProducts = typeof cliOptions.limit === 'number'
    ? feedProducts.slice(0, cliOptions.limit)
    : feedProducts;

  const googleProducts = selectedProducts.flatMap((item) =>
    mapFeedProductToGoogleProduct(item, mappingOptions, categoryMap, manufacturerMap),
  );
  console.log(`Prepared ${googleProducts.length} variant products for upload.`);

  const contentApi = await createContentClient();
  await uploadProducts(googleProducts, merchantId, contentApi, cliOptions.dryRun);
}

main().catch((error) => {
  console.error('Fatal error while syncing Google Merchant Center products:', error);
  process.exit(1);
});

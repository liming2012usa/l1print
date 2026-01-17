import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { google } from 'googleapis';
import { parseStringPromise } from 'xml2js';
import dotenv from 'dotenv';
import type { JWTInput } from 'google-auth-library';

interface DeleteCliOptions {
  xmlPath: string;
  dryRun: boolean;
}

interface MappingOptions {
  channel: 'online' | 'local';
  contentLanguage: string;
  targetCountry: string;
}

interface FeedProduct {
  id?: string;
  code?: string;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

initializeEnv();

function initializeEnv() {
  const lookupPaths = [
    path.resolve(process.cwd(), '.env'),
    path.resolve(__dirname, '.env'),
    path.resolve(__dirname, '..', '.env'),
  ];

  for (const envPath of lookupPaths) {
    const result = dotenv.config({ path: envPath });
    if (!result.error) {
      break;
    }
  }
}

function parseCliArgs(args: string[]): DeleteCliOptions {
  let xmlPath = path.resolve(__dirname, 'data_feeds', 'products-delete.xml');
  let dryRun = false;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === '--file' && args[i + 1]) {
      xmlPath = resolvePath(args[i + 1]);
      i += 1;
    } else if (arg.startsWith('--file=')) {
      const [, value] = arg.split('=');
      xmlPath = resolvePath(value);
    } else if (arg === '--dry-run') {
      dryRun = true;
    }
  }

  return { xmlPath, dryRun };
}

function resolvePath(inputPath?: string): string {
  if (!inputPath) {
    return path.resolve(__dirname, 'data_feeds', 'products-delete.xml');
  }
  return path.isAbsolute(inputPath)
    ? inputPath
    : path.resolve(process.cwd(), inputPath);
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

async function loadDeleteList(xmlPath: string): Promise<string[]> {
  const content = await readFile(xmlPath, 'utf-8');
  const parsed = await parseStringPromise(content, {
    explicitArray: false,
    mergeAttrs: true,
    trim: true,
  });

  const feedProducts = parsed?.products?.product;
  if (!feedProducts) {
    return [];
  }

  const entries: FeedProduct[] = Array.isArray(feedProducts) ? feedProducts : [feedProducts];
  const offerIds = entries
    .map((item) => item.code || item.id)
    .filter((value): value is string => Boolean(value));
  return Array.from(new Set(offerIds));
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

function createRestProductId(offerId: string, mapping: MappingOptions): string {
  return `${mapping.channel}:${mapping.contentLanguage}:${mapping.targetCountry}:${offerId}`;
}

async function deleteProducts(
  offerIds: string[],
  merchantId: string,
  mapping: MappingOptions,
  dryRun: boolean,
) {
  if (!offerIds.length) {
    console.log('No offer IDs found in the delete file.');
    return;
  }

  const contentApi = await createContentClient();

  for (const offerId of offerIds) {
    const restId = createRestProductId(offerId, mapping);
    if (dryRun) {
      console.log(`[dry-run] Would delete product ${restId}`);
      continue;
    }
    try {
      await contentApi.products.delete({
        merchantId,
        productId: restId,
      });
      console.log(`Deleted product ${restId}`);
    } catch (error) {
      console.error(`Failed to delete product ${restId}:`, error);
    }
  }
}

async function main() {
  const cliOptions = parseCliArgs(process.argv.slice(2));
  const merchantId = getEnvVar('GOOGLE_MERCHANT_ID');
  const mapping: MappingOptions = {
    channel: (process.env.GOOGLE_CHANNEL ?? 'online') as 'online' | 'local',
    contentLanguage: process.env.GOOGLE_CONTENT_LANGUAGE ?? 'en',
    targetCountry: process.env.GOOGLE_TARGET_COUNTRY ?? 'US',
  };

  console.log(`Loading delete list from ${cliOptions.xmlPath}`);
  const offerIds = await loadDeleteList(cliOptions.xmlPath);
  console.log(`Found ${offerIds.length} offer IDs slated for deletion.`);

  await deleteProducts(offerIds, merchantId, mapping, cliOptions.dryRun);
}

main().catch((error) => {
  console.error('Fatal error while deleting Google Merchant Center products:', error);
  process.exit(1);
});

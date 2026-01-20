## Google Merchant Center Upload Script

This repository now includes `upload_to_gmc.ts`, a TypeScript script that parses `data_feeds/products.xml`, maps each DecoNetwork product into a [Google Merchant Center Product](https://developers.google.com/shopping-content/reference/rest/v2.1/products), and uploads it through the Content API.

### Prerequisites

1. Install dependencies:
   ```bash
   npm install
   ```
2. Create or reuse a Google Cloud service account with access to the Content API and add it as an admin user in your Merchant Center account.
3. Provide credentials via either:
   - `GOOGLE_APPLICATION_CREDENTIALS` pointing to the downloaded service-account JSON file, **or**
   - `GOOGLE_SERVICE_ACCOUNT_JSON` containing the raw JSON string.

### Required environment variables

| Variable | Description |
| --- | --- |
| `GOOGLE_MERCHANT_ID` | Numeric Merchant Center account ID that the service account can manage. |
| `STORE_BASE_URL` | Base URL that customers use to view products (e.g., `https://l1print.com/`). |

### Optional environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `STORE_ASSET_BASE_URL` | `STORE_BASE_URL` | Used to turn relative image paths into absolute URLs. |
| `PRODUCT_PATH_TEMPLATE` | `/product/{id}` | Relative path template used to build the product `link`. Tokens: `{id}`, `{code}`, `{nameSlug}`. |
| `GOOGLE_CONTENT_LANGUAGE` | `en` | Product `contentLanguage`. |
| `GOOGLE_TARGET_COUNTRY` | `US` | Product `targetCountry`. |
| `GOOGLE_CHANNEL` | `online` | Product `channel` (`online` or `local`). |
| `GOOGLE_PRICE_CURRENCY` | `USD` | Currency for generated `price` values. |
| `GOOGLE_DEFAULT_AVAILABILITY` | `in stock` | Availability value when the feed does not provide inventory details. |
| `GOOGLE_PRODUCT_CONDITION` | `new` | Condition field for each product. |
| `GOOGLE_DEFAULT_PRODUCT_CATEGORY` | _(unset)_ | Optional Google product category ID/string applied to all uploads. |

### Running the script

Use `ts-node` (or the provided npm script) to execute the TypeScript file directly:

```bash
npm run upload:gmc:ts -- --dry-run
```

This is equivalent to `node --loader ts-node/esm upload_to_gmc.ts --dry-run`. Remove `--dry-run` to send real API requests. Common flags:

- `--xml ./path/to/products.xml` – override the feed path (defaults to `./data_feeds/products.xml`).
- `--meta ./path/to/meta_data.xml` – override the category metadata path (defaults to `./data_feeds/meta_data.xml`).
- `--limit 25` – only process the first N products.
- `--infer-description` – allow gender/age heuristics to examine the product description (default is *off*; only the name/code/categories are scanned).
- `--dry-run` – validate parsing/mapping without creating products in GMC.

### What the script does

1. Loads `products.xml` and converts each `<product>` node into a structured object.
2. Parses `meta_data.xml` to resolve category IDs into readable names, then assigns them to Google `productTypes`.
3. Builds canonical product URLs using your `STORE_BASE_URL` and `PRODUCT_PATH_TEMPLATE`.
4. Expands DecoNetwork image placeholders (`[COLOR_ID]`, `[SIZE]`, etc.) using each product’s default variant so Google sees concrete URLs, then normalizes descriptions, size lists, colors, and prices.
5. Authenticates with Google via the Content API.
6. Uploads each mapped product (or logs what would be uploaded when using `--dry-run`).

Review Google Merchant Center errors in the console output; rerun with `--dry-run` to debug mapping without API calls.

### Deleting legacy products

If you need to remove older single-size products, list their `code`/`id` in `data_feeds/products-delete.xml` (same structure as `products.xml`) and run:

```bash
npm run delete:gmc -- --file data_feeds/products-delete.xml --dry-run
```

Drop `--dry-run` once you verify the IDs. The delete script does not enumerate live GMC inventory; it only removes the offer IDs you explicitly list.

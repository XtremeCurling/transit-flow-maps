import { mkdir, copyFile, access, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const repoRoot = path.resolve(__dirname, "..", "..");
const sourceDir = path.join(repoRoot, "data", "processed", "web");
const destDir = path.join(__dirname, "..", "public", "data", "processed", "web");

const GEOJSON_FILES = ["corridor.geojson", "physical.geojson"];
const EMPTY_FEATURE_COLLECTION = '{"type":"FeatureCollection","features":[]}';

async function exists(filePath) {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function main() {
  await mkdir(destDir, { recursive: true });

  for (const fileName of GEOJSON_FILES) {
    const src = path.join(sourceDir, fileName);
    const dst = path.join(destDir, fileName);
    if (await exists(src)) {
      await copyFile(src, dst);
      console.log(`synced ${fileName}`);
    } else {
      await writeFile(dst, EMPTY_FEATURE_COLLECTION, "utf-8");
      console.warn(`missing ${fileName} in data/processed/web; wrote empty placeholder`);
    }
  }
}

main().catch((error) => {
  console.error("failed to sync processed GeoJSON files");
  console.error(error);
  process.exitCode = 1;
});

import { register } from 'node:module';
import { pathToFileURL } from 'node:url';

// Register TypeScript loader
register('ts-node/esm', pathToFileURL('./'));

// This will ensure .ts files are properly loaded
process.env.NODE_OPTIONS = '--loader ts-node/esm';

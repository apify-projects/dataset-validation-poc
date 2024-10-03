// Apify SDK - toolkit for building Apify Actors (Read more at https://docs.apify.com/sdk/js/)
import { Actor } from 'apify';

import { ValidatedPusher } from './push-data.js';

// The init() call configures the Actor for its environment. It's recommended to start every Actor with an init()
await Actor.init();

// NOTE: We are witing for multidataset validation/monitoring for this to work really nicely
// https://github.com/apify/actor-whitepaper/pull/25
const validatedPusher = await ValidatedPusher.create();

const pushData1 = await validatedPusher.pushData({ url: 'test' });

const pushData2 = await validatedPusher.pushData({ name: 'test', url: 1 });

const pushData3 = await validatedPusher.pushData([{ name: 'test', url: 1, nonsense: 1 }, { name: 'test', url: 1, nonsense: 1 }]);

console.dir(pushData1);
console.dir(pushData2);
console.dir(pushData3);

console.log(`Stats`);
console.dir(validatedPusher.getStats());
await Actor.setValue('VALIDATION_STATS', validatedPusher.getStats());

const statsUrl = `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/VALIDATION_STATS`;
const fullDatasetUrl = `https://console.apify.com/storage/datasets/${validatedPusher.fullDataset.id}`;
const errorDatasetUrl = `https://console.apify.com/storage/datasets/${validatedPusher.errorDataset.id}`;

await Actor.exit(`${validatedPusher.getStats().validItems}/${validatedPusher.getStats().invalidItems}`
    + `/${validatedPusher.getStats().invalidItems} valid/invalid/total.\n`
    + `Fields Stats: ${statsUrl}\n`
    + `Full dataset: ${fullDatasetUrl}\n`
    + `Error dataset: ${errorDatasetUrl}`);

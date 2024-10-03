import { Actor, Dataset } from 'apify';
import { useState } from 'crawlee';

// TODO: Implement the schema specs and also utilize it fully in stats
// http://json-schema.org/draft-07/schema#
interface ValidationError {
    instancePath: string; // '',
    schemaPath: string; // '#/required',
    keyword: string; // 'required',
    params: {
        missingProperty?: string;
        type?: 'string' | 'integer' | 'boolean' | 'object' | 'array';
    };
    message: string; // "must have required property 'name'"
}

interface InvalidItem {
    itemPosition: number;
    validationErrors: ValidationError[];
}

// There are a lot of fields actually in this error
interface PushDataError extends Error {
    data: {
        invalidItems: InvalidItem[];
    }
}

interface ValidationStats {
    totalItems: number;
    invalidItems: number;
    validItems: number;
    invalidFields: Record<string, number>;
    invalidKinds: Record<string, number>;
}

type Item = Record<string, unknown>;

export class ValidatedPusher {
    private constructor(
        readonly validatedDataset: Dataset,
        readonly fullDataset: Dataset,
        readonly errorDataset: Dataset,

        private stats: ValidationStats,
    ) { /* private constructor */ }

    static async create() {
        // Default dataset with items only passing validation
        const validatedDataset = await Actor.openDataset();
        // Dataset with all items which we send to customer
        const fullDataset = await Actor.openDataset(`${Actor.getEnv().actorRunId}-full`);
        // Error dataset with errors for each push
        const errorDataset = await Actor.openDataset(`${Actor.getEnv().actorRunId}-validation-errors`);

        const stats = await useState<ValidationStats>('VALIDATION_STATS', {
            totalItems: 0,
            invalidItems: 0,
            validItems: 0,
            invalidFields: {},
            invalidKinds: {},
        });

        return new ValidatedPusher(validatedDataset, fullDataset, errorDataset, stats);
    }

    // TODO:
    // 1. If there is a mix of valid and invalid items, the API (I think) will return an error and not store anything.
    // We need to parse this and store the valid items again.
    // 2. This also means the calculation of stats.validItems etc. is not correct since they can be mixed in one push.
    // 3. Add offset index to the error object so it points to the item in the full dataset. Or we could just add the actual item to each error item
    // 4. Should we create one more invalid dataset for only invalid items? Might be overkill since we already have the full dataset

    pushData = async (data: Item | Item[]): Promise<{ error?: PushDataError, invalidItems: InvalidItem[] }> => {
        let error: PushDataError | undefined;
        let invalidItems: InvalidItem[] = [];

        try {
            await this.validatedDataset.pushData(data);
            this.stats.validItems += Array.isArray(data) ? data.length : 1;
        } catch (err) {
            error = err as PushDataError;
        }

        await this.fullDataset.pushData(data);
        this.stats.totalItems += Array.isArray(data) ? data.length : 1;

        if (error) {
            invalidItems = error.data.invalidItems;

            await this.errorDataset.pushData(invalidItems);

            this.stats.invalidItems += invalidItems.length;

            invalidItems.forEach(({ validationErrors }) => {
                validationErrors.forEach(({ instancePath, keyword, params }) => {
                    let field = instancePath.replace('/', '');

                    if (!field) {
                        if (params.missingProperty) {
                            field = params.missingProperty;
                        }
                    }
                    this.stats.invalidFields[field] = (this.stats.invalidFields[field] ?? 0) + 1;

                    this.stats.invalidKinds[keyword] = (this.stats.invalidKinds[keyword] ?? 0) + 1;
                });
            });
        }

        return { invalidItems, error };
    };

    getStats = () => this.stats;
}

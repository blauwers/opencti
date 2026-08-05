import { describe, expect, it } from 'vitest';
import { extractStixRelationEndpoints } from '../../../src/graphql/sseMiddleware';

describe('sseMiddleware relation endpoint extraction', () => {
  it('extracts relationship endpoints', () => {
    expect(extractStixRelationEndpoints({
      type: 'relationship',
      source_ref: 'indicator--source',
      target_ref: 'malware--target',
    })).toEqual({
      fromId: 'indicator--source',
      toId: 'malware--target',
    });
  });

  it('extracts sighting endpoints', () => {
    expect(extractStixRelationEndpoints({
      type: 'sighting',
      sighting_of_ref: 'indicator--source',
      where_sighted_refs: ['identity--target'],
    })).toEqual({
      fromId: 'indicator--source',
      toId: 'identity--target',
    });
  });

  it.each([
    { type: 'sighting', sighting_of_ref: 'indicator--source' },
    { type: 'sighting', sighting_of_ref: 'indicator--source', where_sighted_refs: [] },
    { type: 'sighting', sighting_of_ref: 'indicator--source', where_sighted_refs: null },
  ])('returns undefined for sightings without a where-sighted endpoint', (stixData) => {
    expect(extractStixRelationEndpoints(stixData)).toBeUndefined();
  });
});

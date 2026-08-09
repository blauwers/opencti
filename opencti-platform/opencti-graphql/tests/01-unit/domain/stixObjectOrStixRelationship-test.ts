import { beforeEach, describe, expect, it, vi } from 'vitest';
import { stixObjectOrRelationshipAddRefRelations, stixObjectOrRelationshipDeleteRefRelations } from '../../../src/domain/stixObjectOrStixRelationship';

const mocks = vi.hoisted(() => ({
  convertDatabaseNameToInputName: vi.fn(),
  storeLoadById: vi.fn(),
  storeLoadByIdWithRefs: vi.fn(),
  transformPatchToInput: vi.fn(),
  updateAttributeFromLoadedWithRefs: vi.fn(),
  validateCreatedBy: vi.fn(),
  validateMarking: vi.fn(),
  validateMarkings: vi.fn(),
}));

vi.mock('../../../src/database/middleware-loader', () => ({
  pageEntitiesOrRelationsConnection: vi.fn(),
  storeLoadById: mocks.storeLoadById,
}));

vi.mock('../../../src/database/middleware', () => ({
  storeLoadByIdWithRefs: mocks.storeLoadByIdWithRefs,
  transformPatchToInput: mocks.transformPatchToInput,
  updateAttributeFromLoadedWithRefs: mocks.updateAttributeFromLoadedWithRefs,
  validateCreatedBy: mocks.validateCreatedBy,
}));

vi.mock('../../../src/schema/schema-relationsRef', () => ({
  schemaRelationsRefDefinition: {
    convertDatabaseNameToInputName: mocks.convertDatabaseNameToInputName,
  },
}));

vi.mock('../../../src/utils/access', () => ({
  validateMarking: mocks.validateMarking,
  validateMarkings: mocks.validateMarkings,
}));

describe('stixObjectOrRelationshipAddRefRelations validation', () => {
  const context = {} as any;
  const user = {} as any;

  beforeEach(() => {
    vi.clearAllMocks();
    mocks.convertDatabaseNameToInputName.mockReturnValue('objectMarking');
    mocks.storeLoadById.mockResolvedValue({ entity_type: 'Report' });
    mocks.storeLoadByIdWithRefs.mockResolvedValue({ entity_type: 'Report' });
    mocks.transformPatchToInput.mockReturnValue([]);
    mocks.updateAttributeFromLoadedWithRefs.mockResolvedValue({
      element: { id: 'report--1' },
    });
  });

  it('validates all object markings before applying one bulk relation patch', async () => {
    await stixObjectOrRelationshipAddRefRelations(
      context,
      user,
      'report--1',
      {
        relationship_type: 'object-marking',
        toIds: ['marking-definition--1', 'marking-definition--2'],
      },
      'Stix-Core-Object',
    );

    expect(mocks.validateMarkings).toHaveBeenCalledWith(
      context,
      user,
      ['marking-definition--1', 'marking-definition--2'],
    );
    expect(mocks.updateAttributeFromLoadedWithRefs).toHaveBeenCalledOnce();
  });

  it('does not patch when bulk object marking validation rejects one target', async () => {
    mocks.validateMarkings.mockRejectedValueOnce(new Error('missing marking'));

    await expect(
      stixObjectOrRelationshipAddRefRelations(
        context,
        user,
        'report--1',
        {
          relationship_type: 'object-marking',
          toIds: ['marking-definition--1', 'marking-definition--2'],
        },
        'Stix-Core-Object',
      ),
    ).rejects.toThrow('missing marking');

    expect(mocks.updateAttributeFromLoadedWithRefs).not.toHaveBeenCalled();
  });

  it('validates every created-by target before applying one bulk relation patch', async () => {
    await stixObjectOrRelationshipAddRefRelations(
      context,
      user,
      'report--1',
      {
        relationship_type: 'created-by',
        toIds: ['identity--1', 'identity--2'],
      },
      'Stix-Core-Object',
    );

    expect(mocks.validateCreatedBy).toHaveBeenNthCalledWith(
      1,
      context,
      user,
      'identity--1',
    );
    expect(mocks.validateCreatedBy).toHaveBeenNthCalledWith(
      2,
      context,
      user,
      'identity--2',
    );
    expect(mocks.updateAttributeFromLoadedWithRefs).toHaveBeenCalledOnce();
  });
});

describe('stixObjectOrRelationshipDeleteRefRelations', () => {
  const context = {} as any;
  const user = {} as any;

  beforeEach(() => {
    vi.clearAllMocks();
    mocks.convertDatabaseNameToInputName.mockReturnValue('objectMarking');
    mocks.storeLoadById.mockResolvedValue({ entity_type: 'Report' });
    mocks.storeLoadByIdWithRefs.mockResolvedValue({ entity_type: 'Report' });
    mocks.transformPatchToInput.mockReturnValue([]);
    mocks.updateAttributeFromLoadedWithRefs.mockResolvedValue({
      element: { id: 'report--1' },
    });
  });

  it('applies one aggregate remove patch for valid ref relationships', async () => {
    await stixObjectOrRelationshipDeleteRefRelations(
      context,
      user,
      'report--1',
      {
        relationship_type: 'object-marking',
        toIds: ['marking-definition--1', 'marking-definition--2'],
      },
      'Stix-Core-Object',
    );

    expect(mocks.transformPatchToInput).toHaveBeenCalledWith(
      { objectMarking: ['marking-definition--1', 'marking-definition--2'] },
      { objectMarking: 'remove' },
    );
    expect(mocks.updateAttributeFromLoadedWithRefs).toHaveBeenCalledOnce();
  });

  it('does not patch when the source object cannot be found', async () => {
    mocks.storeLoadById.mockResolvedValueOnce(null);

    await expect(
      stixObjectOrRelationshipDeleteRefRelations(
        context,
        user,
        'report--1',
        {
          relationship_type: 'object-marking',
          toIds: ['marking-definition--1'],
        },
        'Stix-Core-Object',
      ),
    ).rejects.toThrow('Cannot delete the relations');

    expect(mocks.updateAttributeFromLoadedWithRefs).not.toHaveBeenCalled();
  });

  it('does not patch non-ref relationships through the bulk delete endpoint', async () => {
    await expect(
      stixObjectOrRelationshipDeleteRefRelations(
        context,
        user,
        'report--1',
        {
          relationship_type: 'uses',
          toIds: ['tool--1'],
        },
        'Stix-Core-Object',
      ),
    ).rejects.toThrow('Only stix-ref-relationship');

    expect(mocks.updateAttributeFromLoadedWithRefs).not.toHaveBeenCalled();
  });
});

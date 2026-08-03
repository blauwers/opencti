import { describe, expect, it, vi } from 'vitest';
import { buildLocalMustFilter, classifyBufferedEngineBulkResponseItems, coalesceBufferedEngineBulkActions, executeBufferedEngineBulkActionGroup, executeWithBufferedEngineDocumentLanes, extractBulkOperationErrors, isTransitoryError, materializeBufferedEngineBulkActions, prepareElementForIndexing, splitBufferedEngineBulkActions } from '../../../src/database/engine';
import * as engineConfig from '../../../src/database/engine-config';

describe('prepareElementForIndexing testing', () => {
  it('should base trim applied', async () => {
    const element = await prepareElementForIndexing({ name: '  test' });
    expect(element.name).toBe('test');
  });
  it('should inner trim applied', async () => {
    const element = await prepareElementForIndexing({ num: 10, data: { test: '  spacing   ' } });
    expect(element.num).toBe(10);
    expect(element.data.test).toBe('spacing');
  });
  it('should array trim applied', async () => {
    const element = await prepareElementForIndexing({ test: [20, '  trim01  ', '  trim 02    '] });
    expect(element.test).toEqual([20, 'trim01', 'trim 02']);
  });
  it('should inner array trim applied', async () => {
    const element = await prepareElementForIndexing({ test: { values: [20, '  trim01  ', '  trim 02    '] } });
    expect(element.test.values).toEqual([20, 'trim01', 'trim 02']);
  });
  it('should do nothing with date value', async () => {
    const now = new Date();
    const element = await prepareElementForIndexing({ date: now });
    expect(element.date).toEqual(now);
  });
});

describe('buildLocalMustFilter testing', () => {
  it('should buildLocalMustFilter with script be refused by default', () => {
    const scriptFilter = {
      key: ['name'],
      values: [
        'doc.containsKey(\'name.keyword\')',
      ],
      operator: 'script',
    };

    expect(() => buildLocalMustFilter(scriptFilter)).toThrow(/Filter script is not allowed/);
  });

  it('should buildLocalMustFilter with internal_script should work', () => {
    const scriptFilter = {
      key: ['name'],
      values: [
        'doc.containsKey(\'name.keyword\')',
      ],
      operator: 'internal_script',
    };

    const result = buildLocalMustFilter(scriptFilter);

    expect(result).toStrictEqual({
      bool: {
        minimum_should_match: 1,
        should: [
          {
            script: {
              script: "doc.containsKey('name.keyword')",
            },
          },
        ],
      },
    });
  });

  it('buildLocalMustFilter with script should work when enabled', () => {
    vi.spyOn(engineConfig, 'isEsScriptFilterEnabled').mockResolvedValue(true);
    const scriptFilter = {
      key: ['name'],
      values: [
        'doc.containsKey(\'name.keyword\')',
      ],
      operator: 'script',
    };

    const result = buildLocalMustFilter(scriptFilter);

    expect(result).toStrictEqual({
      bool: {
        minimum_should_match: 1,
        should: [
          {
            script: {
              script: "doc.containsKey('name.keyword')",
            },
          },
        ],
      },
    });
  });

  it('should buildLocalMustFilter with contact_information emit a single terms clause for multiple values', () => {
    const emails = ['user@example.com', 'user2@example.com', 'user3@example.com'];
    const filter = {
      key: ['contact_information'],
      values: emails,
      operator: 'eq',
    };

    const result = buildLocalMustFilter(filter);

    expect(result).toStrictEqual({
      bool: {
        minimum_should_match: 1,
        should: [
          {
            terms: { 'contact_information.keyword': emails },
          },
        ],
      },
    });
  });
});

describe('extractBulkOperationErrors testing', () => {
  it('extracts errors from mixed bulk action kinds', () => {
    const indexError = { type: 'index_error' };
    const updateError = { type: 'update_error' };
    const deleteError = { type: 'delete_error' };
    const createError = { type: 'create_error' };

    expect(extractBulkOperationErrors([
      { index: { error: indexError } },
      { update: { error: updateError } },
      { delete: { error: deleteError } },
      { create: { error: createError } },
      { index: { status: 201 } },
    ])).toEqual([indexError, updateError, deleteError, createError]);
  });
});

describe('classifyBufferedEngineBulkResponseItems testing', () => {
  const context = {};
  const actions = [
    {
      context,
      refresh: false,
      body: [
        { index: { _index: 'test_index', _id: 'entity--1' } },
        { internal_id: 'entity--1' },
      ],
    },
    {
      context,
      refresh: false,
      body: [
        { update: { _index: 'test_index', _id: 'entity--2' } },
        { doc: { name: 'updated' } },
      ],
    },
    {
      context,
      refresh: false,
      body: [
        { delete: { _index: 'test_index', _id: 'entity--3' } },
      ],
    },
  ];

  it('keeps response items aligned with the action that must be retried', () => {
    const classified = classifyBufferedEngineBulkResponseItems(actions, [
      { index: { status: 201 } },
      { update: { status: 429, error: { type: 'es_rejected_execution_exception' } } },
      { delete: { status: 404, error: { type: 'document_missing_exception' } } },
    ]);

    expect(classified.permanentFailures).toEqual([]);
    expect(classified.retryableFailures).toHaveLength(1);
    expect(classified.retryableFailures[0].action).toBe(actions[1]);
    expect(classified.retryableFailures[0].error).toEqual({
      type: 'es_rejected_execution_exception',
      status: 429,
    });
  });

  it('rejects malformed bulk responses that cannot be mapped to submitted actions', () => {
    expect(() => classifyBufferedEngineBulkResponseItems(actions, [{ index: { status: 201 } }]))
      .toThrow(/Bulk indexing response does not match submitted actions/);
  });

  it('classifies optimistic concurrency conflicts separately from transient retries', () => {
    const guardedActions = [
      { ...actions[0], versionConflictGroup: { entries: [], key: 'test_index:entity--1' } },
      actions[1],
      actions[2],
    ];
    const classified = classifyBufferedEngineBulkResponseItems(guardedActions, [
      { index: { status: 409, error: { type: 'version_conflict_engine_exception' } } },
      { update: { status: 201 } },
      { delete: { status: 200 } },
    ]);

    expect(classified.permanentFailures).toEqual([]);
    expect(classified.retryableFailures).toEqual([]);
    expect(classified.versionConflicts).toHaveLength(1);
    expect(classified.versionConflicts[0].action).toBe(guardedActions[0]);
  });
});

describe('executeBufferedEngineBulkActionGroup testing', () => {
  const context = {};
  const actions = [
    {
      context,
      refresh: false,
      body: [
        { index: { _index: 'test_index', _id: 'entity--1' } },
        { internal_id: 'entity--1' },
      ],
    },
    {
      context,
      refresh: false,
      body: [
        { index: { _index: 'test_index', _id: 'entity--2' } },
        { internal_id: 'entity--2' },
      ],
    },
  ];

  it('retries only transient failed items from a partial bulk response', async () => {
    const executeBulk = vi.fn()
      .mockResolvedValueOnce({
        errors: true,
        items: [
          { index: { status: 201 } },
          { index: { status: 429, error: { type: 'es_rejected_execution_exception' } } },
        ],
      })
      .mockResolvedValueOnce({
        errors: false,
        items: [{ index: { status: 201 } }],
      });
    const waitForRetry = vi.fn().mockResolvedValue(undefined);

    await executeBufferedEngineBulkActionGroup(actions, {
      executeBulk,
      random: () => 0,
      waitForRetry,
    });

    expect(executeBulk).toHaveBeenCalledTimes(2);
    expect(executeBulk.mock.calls[0][1].body).toEqual([...actions[0].body, ...actions[1].body]);
    expect(executeBulk.mock.calls[1][1].body).toEqual(actions[1].body);
    expect(waitForRetry).toHaveBeenCalledWith(250);
  });

  it('fails permanent item errors immediately without replaying successful actions', async () => {
    const executeBulk = vi.fn().mockResolvedValue({
      errors: true,
      items: [
        { index: { status: 201 } },
        { index: { status: 400, error: { type: 'mapper_parsing_exception' } } },
      ],
    });

    await expect(executeBufferedEngineBulkActionGroup(actions, {
      executeBulk,
      waitForRetry: vi.fn().mockResolvedValue(undefined),
    })).rejects.toMatchObject({
      extensions: {
        data: {
          errors: [{
            action: 'index',
            documentId: 'entity--2',
            index: 'test_index',
            status: 400,
            type: 'mapper_parsing_exception',
          }],
        },
      },
    });
    expect(executeBulk).toHaveBeenCalledTimes(1);
  });

  it('replans only optimistic concurrency conflicts before retrying', async () => {
    const guardedActions = [
      { ...actions[0], versionConflictGroup: { entries: [], key: 'test_index:entity--1' } },
      actions[1],
    ];
    const replannedAction = {
      ...guardedActions[0],
      body: [
        { index: { _index: 'test_index', _id: 'entity--1', if_seq_no: 11, if_primary_term: 4 } },
        { internal_id: 'entity--1', name: 'replanned' },
      ],
    };
    const executeBulk = vi.fn()
      .mockResolvedValueOnce({
        errors: true,
        items: [
          { index: { status: 409, error: { type: 'version_conflict_engine_exception' } } },
          { index: { status: 201 } },
        ],
      })
      .mockResolvedValueOnce({
        errors: false,
        items: [{ index: { status: 200 } }],
      });
    const replanVersionConflicts = vi.fn().mockResolvedValue([replannedAction]);
    const waitForRetry = vi.fn().mockResolvedValue(undefined);

    await executeBufferedEngineBulkActionGroup(guardedActions, {
      executeBulk,
      random: () => 0,
      replanVersionConflicts,
      waitForRetry,
    });

    expect(replanVersionConflicts).toHaveBeenCalledTimes(1);
    expect(replanVersionConflicts.mock.calls[0][0]).toHaveLength(1);
    expect(replanVersionConflicts.mock.calls[0][0][0].action).toBe(guardedActions[0]);
    expect(executeBulk).toHaveBeenCalledTimes(2);
    expect(executeBulk.mock.calls[1][1].body).toEqual(replannedAction.body);
    expect(waitForRetry).toHaveBeenCalledWith(250);
  });
});

describe('executeWithBufferedEngineDocumentLanes testing', () => {
  const context = {};
  const buildAction = (id) => ({
    context,
    refresh: false,
    body: [
      { index: { _index: 'test_index', _id: id } },
      { internal_id: id },
    ],
  });

  it('serializes overlapping document IDs while unrelated IDs continue', async () => {
    const order = [];
    let releaseFirst = () => {};
    let signalFirstStarted = () => {};
    let signalThirdStarted = () => {};
    const firstBlocked = new Promise((resolve) => {
      releaseFirst = resolve;
    });
    const firstStarted = new Promise((resolve) => {
      signalFirstStarted = resolve;
    });
    const thirdStarted = new Promise((resolve) => {
      signalThirdStarted = resolve;
    });

    const first = executeWithBufferedEngineDocumentLanes([buildAction('entity--1')], async () => {
      order.push('first-start');
      signalFirstStarted();
      await firstBlocked;
      order.push('first-end');
    });
    await firstStarted;
    const second = executeWithBufferedEngineDocumentLanes([buildAction('entity--1')], async () => {
      order.push('second-start');
    });
    const third = executeWithBufferedEngineDocumentLanes([buildAction('entity--2')], async () => {
      order.push('third-start');
      signalThirdStarted();
    });
    await thirdStarted;

    expect(order).toEqual(['first-start', 'third-start']);
    releaseFirst();
    await Promise.all([first, second, third]);
    expect(order).toEqual(['first-start', 'third-start', 'first-end', 'second-start']);
  });
});

describe('coalesceBufferedEngineBulkActions testing', () => {
  const context = {};

  it('combines repeated same-document updates into one ordered script', () => {
    const actions = [
      {
        context,
        refresh: true,
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { script: { source: 'ctx._source.name = params.name', params: { name: 'first' } } },
        ],
      },
      {
        context,
        refresh: true,
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { doc: { description: 'second' } },
        ],
      },
      {
        context,
        refresh: true,
        body: [
          { update: { _index: 'test_index', _id: 'entity--2' } },
          { doc: { name: 'other' } },
        ],
      },
    ];

    const compacted = coalesceBufferedEngineBulkActions(actions);

    expect(compacted).toHaveLength(2);
    expect(compacted[0].body[0]).toEqual({ update: { _index: 'test_index', _id: 'entity--1' } });
    expect(compacted[0].body[1].script.source).toContain('ctx._source.name = params.b1_0_0');
    expect(compacted[0].body[1].script.source).toContain("ctx._source['description'] = params['b1_1_0']");
    expect(compacted[0].body[1].script.params).toEqual({
      b1_0_0: 'first',
      b1_1_0: 'second',
    });
    expect(compacted[1]).toEqual(actions[2]);
  });

  it('keeps scope-sensitive scripts separate instead of composing invalid Painless', () => {
    const actions = [
      {
        context,
        refresh: true,
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { script: { source: 'def values = params.values; ctx._source.first = values', params: { values: ['first'] } } },
        ],
      },
      {
        context,
        refresh: true,
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { script: { source: 'def values = params.values; ctx._source.second = values', params: { values: ['second'] } } },
        ],
      },
    ];

    const compacted = coalesceBufferedEngineBulkActions(actions);

    expect(compacted).toEqual(actions);
  });

  it('folds updates into a buffered index document when the batch owns its state', () => {
    const actions = [
      {
        context,
        refresh: true,
        body: [
          { index: { _index: 'test_index', _id: 'entity--1' } },
          { internal_id: 'entity--1', name: 'created' },
        ],
      },
      {
        context,
        refresh: true,
        applyToDocument: (existing) => ({ ...existing, description: 'updated' }),
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { doc: { description: 'updated' } },
        ],
      },
      {
        context,
        refresh: true,
        applyToDocument: (existing) => ({ ...existing, confidence: 88 }),
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { doc: { confidence: 88 } },
        ],
      },
    ];

    const compacted = coalesceBufferedEngineBulkActions(actions);

    expect(compacted).toHaveLength(1);
    expect(compacted[0].body).toEqual([
      { index: { _index: 'test_index', _id: 'entity--1' } },
      { internal_id: 'entity--1', name: 'created', description: 'updated', confidence: 88 },
    ]);
  });

  it('keeps update actions separate across document barriers or unsupported bodies', () => {
    const actions = [
      {
        context,
        refresh: true,
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { doc: { name: 'first' } },
        ],
      },
      {
        context,
        refresh: true,
        body: [
          { index: { _index: 'test_index', _id: 'entity--1' } },
          { internal_id: 'entity--1', name: 'replacement' },
        ],
      },
      {
        context,
        refresh: true,
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { doc: { description: 'after-index' } },
        ],
      },
      {
        context,
        refresh: true,
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { script: { source: 'ctx._source.score = params.score', params: { score: 42 } }, upsert: {} },
        ],
      },
    ];

    expect(coalesceBufferedEngineBulkActions(actions)).toEqual(actions);
  });
});

describe('splitBufferedEngineBulkActions testing', () => {
  const context = {};
  const buildAction = (id, value) => ({
    context,
    refresh: false,
    body: [
      { index: { _index: 'test_index', _id: id } },
      { internal_id: id, value },
    ],
  });
  const actionByteLength = (action) => action.body.reduce((total, line) => total + Buffer.byteLength(JSON.stringify(line)) + 1, 0);

  it('splits complete actions at the configured operation limit', () => {
    const actions = [
      buildAction('entity--1', 'first'),
      buildAction('entity--2', 'second'),
      buildAction('entity--3', 'third'),
    ];

    expect(splitBufferedEngineBulkActions(actions, 2, Number.MAX_SAFE_INTEGER)).toEqual([
      [actions[0], actions[1]],
      [actions[2]],
    ]);
  });

  it('flushes before the next action would exceed the NDJSON byte budget', () => {
    const actions = [
      buildAction('entity--1', 'first'),
      buildAction('entity--2', 'second'),
    ];
    const maxBytes = actionByteLength(actions[0]) + actionByteLength(actions[1]) - 1;

    expect(splitBufferedEngineBulkActions(actions, 5000, maxBytes)).toEqual([
      [actions[0]],
      [actions[1]],
    ]);
  });

  it('keeps a single oversized action intact', () => {
    const action = buildAction('entity--1', 'oversized');

    expect(splitBufferedEngineBulkActions([action], 5000, actionByteLength(action) - 1)).toEqual([[action]]);
  });
});

describe('materializeBufferedEngineBulkActions testing', () => {
  it('keeps a single update action intact so Elasticsearch can still detect no-ops', () => {
    const action = {
      context: {},
      refresh: true,
      applyToDocument: (existing) => ({ ...existing, description: 'same' }),
      body: [
        { update: { _index: 'test_index', _id: 'entity--1' } },
        { doc: { description: 'same' } },
      ],
    };

    expect(materializeBufferedEngineBulkActions([action], {
      documents: new Map([['test_index:entity--1', { internal_id: 'entity--1', description: 'same' }]]),
      loadedKeys: new Set(['test_index:entity--1']),
      versions: new Map(),
    })).toEqual([action]);
  });

  it('folds update-only document mutations from different contexts into one final index action', () => {
    const actions = [
      {
        context: { requestId: 'first' },
        refresh: true,
        applyToDocument: (existing) => ({ ...existing, description: 'first' }),
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { doc: { description: 'first' } },
        ],
      },
      {
        context: { requestId: 'second' },
        refresh: true,
        applyToDocument: (existing) => ({ ...existing, confidence: 88 }),
        body: [
          { update: { _index: 'test_index', _id: 'entity--1' } },
          { doc: { confidence: 88 } },
        ],
      },
    ];

    const materialized = materializeBufferedEngineBulkActions(actions, {
      documents: new Map([['test_index:entity--1', { internal_id: 'entity--1', name: 'existing' }]]),
      loadedKeys: new Set(['test_index:entity--1']),
      versions: new Map([['test_index:entity--1', { seqNo: 12, primaryTerm: 3 }]]),
    });

    expect(materialized).toHaveLength(1);
    expect(materialized[0].body).toEqual([
      { index: { _index: 'test_index', _id: 'entity--1', if_seq_no: 12, if_primary_term: 3 } },
      { internal_id: 'entity--1', name: 'existing', description: 'first', confidence: 88 },
    ]);
  });

  it('uses create when the final document state is confirmed new in the batch prefetch', () => {
    const actions = [
      {
        context: {},
        refresh: true,
        body: [
          { index: { _index: 'test_index', _id: 'entity--1', retry_on_conflict: 30 } },
          { internal_id: 'entity--1', name: 'created' },
        ],
      },
      {
        context: {},
        refresh: true,
        applyToDocument: (existing) => ({ ...existing, description: 'after-create' }),
        body: [
          { update: { _index: 'test_index', _id: 'entity--1', retry_on_conflict: 30 } },
          { doc: { description: 'after-create' } },
        ],
      },
    ];

    expect(materializeBufferedEngineBulkActions(actions, {
      documents: new Map(),
      loadedKeys: new Set(['test_index:entity--1']),
      versions: new Map(),
    })[0].body).toEqual([
      { create: { _index: 'test_index', _id: 'entity--1' } },
      { internal_id: 'entity--1', name: 'created', description: 'after-create' },
    ]);
  });

  it('aggregates indexed relationship impacts before applying the final document state', () => {
    const actions = [
      {
        context: {},
        refresh: true,
        applyToDocument: () => {
          throw new Error('relation impacts should be aggregated');
        },
        finalStateMutation: {
          kind: 'indexed-relation-impact',
          targetsElements: [{
            relation: 'related-to',
            field: 'internal_id',
            elements: [{ id: 'indicator--1', side: 'from', type: 'Indicator' }],
          }],
          updatedAt: '2026-08-03T00:00:00.000Z',
        },
        body: [
          { update: { _index: 'test_index', _id: 'report--1' } },
          { script: { source: 'first' } },
        ],
      },
      {
        context: {},
        refresh: true,
        applyToDocument: () => {
          throw new Error('relation impacts should be aggregated');
        },
        finalStateMutation: {
          kind: 'indexed-relation-impact',
          targetsElements: [{
            relation: 'related-to',
            field: 'internal_id',
            elements: [{ id: 'indicator--2', side: 'from', type: 'Indicator' }],
          }],
          updatedAt: '2026-08-03T00:00:01.000Z',
        },
        body: [
          { update: { _index: 'test_index', _id: 'report--1' } },
          { script: { source: 'second' } },
        ],
      },
    ];

    const materialized = materializeBufferedEngineBulkActions(actions, {
      documents: new Map([['test_index:report--1', {
        internal_id: 'report--1',
        'rel_related-to.internal_id': ['indicator--0'],
      }]]),
      loadedKeys: new Set(['test_index:report--1']),
      versions: new Map(),
    });

    expect(materialized).toHaveLength(1);
    expect(materialized[0].body[1]['rel_related-to.internal_id']).toEqual(['indicator--0', 'indicator--1', 'indicator--2']);
  });

  it('drops repeated relation impact updates when only projection timestamps would change', () => {
    const actions = Array.from({ length: 2 }, (_, index) => ({
      context: {},
      refresh: true,
      applyToDocument: () => {
        throw new Error('relation impacts should be aggregated');
      },
      finalStateMutation: {
        kind: 'indexed-relation-impact',
        targetsElements: [{
          relation: 'object-marking',
          field: 'internal_id',
          elements: [{ id: 'marking-definition--1', side: 'from', type: 'Indicator' }],
        }],
        updatedAt: `2026-08-03T00:00:0${index}.000Z`,
      },
      body: [
        { update: { _index: 'test_index', _id: 'indicator--1' } },
        { script: { source: `impact-${index}` } },
      ],
    }));

    expect(materializeBufferedEngineBulkActions(actions, {
      documents: new Map([['test_index:indicator--1', {
        internal_id: 'indicator--1',
        modified: '2026-08-02T00:00:00.000Z',
        refreshed_at: '2026-08-02T00:00:00.000Z',
        'rel_object-marking.internal_id': ['marking-definition--1'],
        updated_at: '2026-08-02T00:00:00.000Z',
      }]]),
      loadedKeys: new Set(['test_index:indicator--1']),
      versions: new Map(),
    })).toEqual([]);
  });

  it('drops a create followed by a delete when the document did not exist before the batch', () => {
    const actions = [
      {
        context: {},
        refresh: true,
        body: [
          { index: { _index: 'test_index', _id: 'entity--1' } },
          { internal_id: 'entity--1', name: 'created' },
        ],
      },
      {
        context: {},
        refresh: true,
        body: [
          { delete: { _index: 'test_index', _id: 'entity--1' } },
        ],
      },
    ];

    expect(materializeBufferedEngineBulkActions(actions, {
      documents: new Map(),
      loadedKeys: new Set(['test_index:entity--1']),
      versions: new Map(),
    })).toEqual([]);
  });
});

describe('isTransitoryError testing', () => {
  // ── Status code branch ─────────────────────────────────────────────────────

  it('should return true for statusCode 429 on root error', () => {
    expect(isTransitoryError({ statusCode: 429 })).toBe(true);
  });

  it('should return true for statusCode 503 on root error', () => {
    expect(isTransitoryError({ statusCode: 503 })).toBe(true);
  });

  it('should return true for statusCode 429 via meta.statusCode', () => {
    expect(isTransitoryError({ meta: { statusCode: 429 } })).toBe(true);
  });

  it('should return true for statusCode 503 via meta.statusCode', () => {
    expect(isTransitoryError({ meta: { statusCode: 503 } })).toBe(true);
  });

  it('should return true for statusCode 429 via status field', () => {
    expect(isTransitoryError({ status: 429 })).toBe(true);
  });

  it('should return true for statusCode 503 via status field', () => {
    expect(isTransitoryError({ status: 503 })).toBe(true);
  });

  it('should return true for statusCode 429 via cause.statusCode', () => {
    expect(isTransitoryError({ cause: { statusCode: 429 } })).toBe(true);
  });

  it('should return true for statusCode 503 via cause.meta.statusCode', () => {
    expect(isTransitoryError({ cause: { meta: { statusCode: 503 } } })).toBe(true);
  });

  it('should return true for statusCode 429 via extensions.data.cause.statusCode', () => {
    expect(isTransitoryError({ extensions: { data: { cause: { statusCode: 429 } } } })).toBe(true);
  });

  it('should return true for statusCode 503 via extensions.data.cause.meta.statusCode', () => {
    expect(isTransitoryError({ extensions: { data: { cause: { meta: { statusCode: 503 } } } } })).toBe(true);
  });

  it('should return false for a non-transitory statusCode (e.g. 500)', () => {
    expect(isTransitoryError({ statusCode: 500 })).toBe(false);
  });

  it('should return false for statusCode 200', () => {
    expect(isTransitoryError({ statusCode: 200 })).toBe(false);
  });

  // ── Error code branch ───────────────────────────────────────────────────────

  it('should return true for ECONNRESET via root code', () => {
    expect(isTransitoryError({ code: 'ECONNRESET' })).toBe(true);
  });

  it('should return true for ECONNREFUSED via root code', () => {
    expect(isTransitoryError({ code: 'ECONNREFUSED' })).toBe(true);
  });

  it('should return true for ETIMEDOUT via root code', () => {
    expect(isTransitoryError({ code: 'ETIMEDOUT' })).toBe(true);
  });

  it('should return true for EPIPE via root code', () => {
    expect(isTransitoryError({ code: 'EPIPE' })).toBe(true);
  });

  it('should return true for EAI_AGAIN via root code', () => {
    expect(isTransitoryError({ code: 'EAI_AGAIN' })).toBe(true);
  });

  it('should return true for ECONNRESET via cause.code', () => {
    expect(isTransitoryError({ cause: { code: 'ECONNRESET' } })).toBe(true);
  });

  it('should return true for ETIMEDOUT via originalError.code', () => {
    expect(isTransitoryError({ originalError: { code: 'ETIMEDOUT' } })).toBe(true);
  });

  it('should return true for ECONNREFUSED via extensions.data.cause.code', () => {
    expect(isTransitoryError({ extensions: { data: { cause: { code: 'ECONNREFUSED' } } } })).toBe(true);
  });

  it('should return false for a non-transitory error code', () => {
    expect(isTransitoryError({ code: 'ENOENT' })).toBe(false);
  });

  // ── Text pattern branch – message field ────────────────────────────────────

  it('should return true when root message contains circuit_breaking_exception', () => {
    expect(isTransitoryError({ message: 'circuit_breaking_exception: [parent] Data too large' })).toBe(true);
  });

  it('should return true when root message contains es_rejected_execution', () => {
    expect(isTransitoryError({ message: 'es_rejected_execution: queue capacity reached' })).toBe(true);
  });

  it('should return true when root message contains too_many_requests', () => {
    expect(isTransitoryError({ message: 'too_many_requests' })).toBe(true);
  });

  it('should return true when root message contains service_unavailable', () => {
    expect(isTransitoryError({ message: 'service_unavailable' })).toBe(true);
  });

  it('should return true when pattern is mixed-case (case-insensitive match)', () => {
    expect(isTransitoryError({ message: 'Circuit_Breaking_Exception occurred' })).toBe(true);
  });

  // ── Text pattern branch – nested paths via collectErrorFieldValues ──────────

  it('should return true when cause.message contains circuit_breaking_exception', () => {
    expect(isTransitoryError({ cause: { message: 'circuit_breaking_exception' } })).toBe(true);
  });

  it('should return true when cause.meta.body.error.message contains circuit_breaking_exception', () => {
    expect(isTransitoryError({ cause: { meta: { body: { error: { message: 'circuit_breaking_exception' } } } } })).toBe(true);
  });

  it('should return true when originalError.message contains es_rejected_execution', () => {
    expect(isTransitoryError({ originalError: { message: 'es_rejected_execution' } })).toBe(true);
  });

  it('should return true when meta.body.error.message contains circuit_breaking_exception', () => {
    expect(isTransitoryError({ meta: { body: { error: { message: 'circuit_breaking_exception' } } } })).toBe(true);
  });

  it('should return true when extensions.data.cause.message contains too_many_requests', () => {
    expect(isTransitoryError({ extensions: { data: { cause: { message: 'too_many_requests' } } } })).toBe(true);
  });

  it('should return true when extensions.data.cause.meta.body.error.message contains circuit_breaking_exception', () => {
    expect(isTransitoryError({
      extensions: { data: { cause: { meta: { body: { error: { message: 'circuit_breaking_exception' } } } } } },
    })).toBe(true);
  });

  it('should return true when extensions.exception.message contains service_unavailable', () => {
    expect(isTransitoryError({ extensions: { exception: { message: 'service_unavailable' } } })).toBe(true);
  });

  // ── Text pattern branch – other field names (reason / type / name / stack) ──

  it('should return true when reason field contains circuit_breaking_exception', () => {
    expect(isTransitoryError({ reason: 'circuit_breaking_exception' })).toBe(true);
  });

  it('should return true when type field contains es_rejected_execution', () => {
    expect(isTransitoryError({ type: 'es_rejected_execution' })).toBe(true);
  });

  it('should return true when name field contains too_many_requests', () => {
    expect(isTransitoryError({ name: 'too_many_requests' })).toBe(true);
  });

  it('should return true when stack field contains circuit_breaking_exception', () => {
    expect(isTransitoryError({ stack: 'ResponseError: circuit_breaking_exception\n at something' })).toBe(true);
  });

  // ── Real-world production error shape (from the reported bug) ──────────────

  it('should return true for the real-world circuit_breaking_exception ResponseError shape', () => {
    const error = {
      code: 'UNKNOWN_ERROR',
      message: 'circuit_breaking_exception\n\tRoot causes:\n\t\tcircuit_breaking_exception: [parent] Data too large, data for [<http_request>] would be [6254606938/5.8gb], which is larger than the limit of [6120328396/5.6gb]',
      name: 'ResponseError',
      stack: 'ResponseError: circuit_breaking_exception\n\tRoot causes:\n\t\tcircuit_breaking_exception: [parent] Data too large',
    };
    expect(isTransitoryError(error)).toBe(true);
  });

  // ── False cases ─────────────────────────────────────────────────────────────

  it('should return false for a plain non-transitory error', () => {
    expect(isTransitoryError({ message: 'index_not_found_exception', code: 'ENOENT', statusCode: 404 })).toBe(false);
  });

  it('should return false for null', () => {
    expect(isTransitoryError(null)).toBe(false);
  });

  it('should return false for undefined', () => {
    expect(isTransitoryError(undefined)).toBe(false);
  });

  it('should return false for an empty object', () => {
    expect(isTransitoryError({})).toBe(false);
  });

  it('should return false when text fields are empty strings (not matched)', () => {
    expect(isTransitoryError({ message: '', reason: '', type: '', name: '', stack: '' })).toBe(false);
  });
});

import { describe, expect, it, vi } from 'vitest';
import { buildBatchRequestTimeoutMiddleware, isBatchMutationsExecuteRequest } from '../../../src/http/httpServer-timeout';

describe('httpServer timeout middleware', () => {
  it('detects the batch mutation by operation name or named mutation query', () => {
    expect(isBatchMutationsExecuteRequest({ operationName: 'BatchMutationsExecute' })).toBe(true);
    expect(isBatchMutationsExecuteRequest({
      query: 'mutation BatchMutationsExecute($operations: [BatchGraphqlOperationInput!]!) { batchMutationsExecute(operations: $operations) { operation_count } }',
    })).toBe(true);
    expect(isBatchMutationsExecuteRequest({ query: 'query ConnectorList { connectors { id } }' })).toBe(false);
    expect(isBatchMutationsExecuteRequest(null)).toBe(false);
  });

  it('extends only batch mutation request and response socket timeouts', () => {
    const middleware = buildBatchRequestTimeoutMiddleware(3600000);
    const requestSetTimeout = vi.fn();
    const responseSetTimeout = vi.fn();
    const next = vi.fn();

    middleware({
      body: { query: 'mutation BatchMutationsExecute { batchMutationsExecute(operations: []) { operation_count } }' },
      setTimeout: requestSetTimeout,
    }, {
      setTimeout: responseSetTimeout,
    }, next);

    expect(requestSetTimeout).toHaveBeenCalledWith(3600000);
    expect(responseSetTimeout).toHaveBeenCalledWith(3600000);
    expect(next).toHaveBeenCalledOnce();

    middleware({
      body: { query: 'query ConnectorList { connectors { id } }' },
      setTimeout: requestSetTimeout,
    }, {
      setTimeout: responseSetTimeout,
    }, next);

    expect(requestSetTimeout).toHaveBeenCalledTimes(1);
    expect(responseSetTimeout).toHaveBeenCalledTimes(1);
    expect(next).toHaveBeenCalledTimes(2);
  });
});

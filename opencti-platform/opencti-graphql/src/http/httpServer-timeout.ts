type TimeoutRequest = {
  body?: unknown;
  setTimeout?: (timeout: number) => unknown;
};

type TimeoutResponse = {
  setTimeout?: (timeout: number) => unknown;
};

const BATCH_MUTATIONS_EXECUTE_OPERATION_NAME = 'BatchMutationsExecute';
const BATCH_MUTATIONS_EXECUTE_QUERY_PATTERN = /\bmutation\s+BatchMutationsExecute\b/;

export const isBatchMutationsExecuteRequest = (body: unknown): boolean => {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    return false;
  }
  const requestBody = body as { operationName?: unknown; query?: unknown };
  if (requestBody.operationName === BATCH_MUTATIONS_EXECUTE_OPERATION_NAME) {
    return true;
  }
  return typeof requestBody.query === 'string'
    && BATCH_MUTATIONS_EXECUTE_QUERY_PATTERN.test(requestBody.query);
};

export const buildBatchRequestTimeoutMiddleware = (timeout: number) => (
  req: TimeoutRequest,
  res: TimeoutResponse,
  next: () => void,
): void => {
  if (timeout > 0 && isBatchMutationsExecuteRequest(req.body)) {
    req.setTimeout?.(timeout);
    res.setTimeout?.(timeout);
  }
  next();
};

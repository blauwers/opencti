import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { v4 as uuid } from 'uuid';
import gql from 'graphql-tag';
import { USER_CONNECTOR, USER_EDITOR } from '../../utils/testQuery';
import {
  queryAsAdminWithSuccess,
  queryAsUserIsExpectedError,
  queryAsUserIsExpectedForbidden,
  queryAsUserWithSuccess,
} from '../../utils/testQueryHelper';

const CREATE_CONNECTOR_QUERY = gql`
  mutation RegisterConnector($input: RegisterConnectorInput) {
    registerConnector(input: $input) {
      id
    }
  }
`;

const DELETE_CONNECTOR_QUERY = gql`
  mutation ConnectorDeletionMutation($id: ID!) {
    deleteConnector(id: $id)
  }
`;

const ACQUIRE_FRESHNESS_QUERY = gql`
  mutation ConnectorFreshnessAcquire($input: ConnectorFreshnessAcquireInput!) {
    connectorFreshnessAcquire(input: $input) {
      key
      status
      lease_token
      retry_after_ms
    }
  }
`;

const COMPLETE_FRESHNESS_QUERY = gql`
  mutation ConnectorFreshnessComplete($input: ConnectorFreshnessCompleteInput!) {
    connectorFreshnessComplete(input: $input)
  }
`;

const connectorId = uuid();
const adminOwnedConnectorId = uuid();

const connectorInput = (id: string, name: string) => ({
  input: {
    id,
    name,
    type: 'INTERNAL_ENRICHMENT',
    scope: 'IPv4-Addr',
    auto: true,
    only_contextual: false,
  },
});

beforeAll(async () => {
  await queryAsUserWithSuccess(USER_CONNECTOR, {
    query: CREATE_CONNECTOR_QUERY,
    variables: connectorInput(connectorId, 'Freshness Connector'),
  });
  await queryAsAdminWithSuccess({
    query: CREATE_CONNECTOR_QUERY,
    variables: connectorInput(adminOwnedConnectorId, 'Admin Freshness Connector'),
  });
});

afterAll(async () => {
  await queryAsUserWithSuccess(USER_CONNECTOR, {
    query: DELETE_CONNECTOR_QUERY,
    variables: { id: connectorId },
  });
  await queryAsAdminWithSuccess({
    query: DELETE_CONNECTOR_QUERY,
    variables: { id: adminOwnedConnectorId },
  });
});

describe('Connector freshness resolver', () => {
  it('should acquire and complete a connector-owned freshness lease', async () => {
    const key = `198.51.100.${Math.floor(Math.random() * 200) + 1}`;
    const acquire = await queryAsUserWithSuccess(USER_CONNECTOR, {
      query: ACQUIRE_FRESHNESS_QUERY,
      variables: {
        input: {
          connector_id: connectorId,
          namespace: 'shodan-internetdb',
          keys: [key],
          lease_ttl_seconds: 30,
        },
      },
    });
    const [decision] = acquire.data.connectorFreshnessAcquire;
    expect(decision.status).toEqual('ACQUIRED');
    expect(decision.lease_token).toBeTruthy();

    const complete = await queryAsUserWithSuccess(USER_CONNECTOR, {
      query: COMPLETE_FRESHNESS_QUERY,
      variables: {
        input: {
          connector_id: connectorId,
          namespace: 'shodan-internetdb',
          key,
          lease_token: decision.lease_token,
          freshness_ttl_seconds: 60,
        },
      },
    });
    expect(complete.data.connectorFreshnessComplete).toBe(true);

    const fresh = await queryAsUserWithSuccess(USER_CONNECTOR, {
      query: ACQUIRE_FRESHNESS_QUERY,
      variables: {
        input: {
          connector_id: connectorId,
          namespace: 'shodan-internetdb',
          keys: [key],
          lease_ttl_seconds: 30,
        },
      },
    });
    expect(fresh.data.connectorFreshnessAcquire[0].status).toEqual('FRESH');
  });

  it('should reject connector-capable callers mutating another owner namespace', async () => {
    await queryAsUserIsExpectedError(USER_CONNECTOR, {
      query: ACQUIRE_FRESHNESS_QUERY,
      variables: {
        input: {
          connector_id: adminOwnedConnectorId,
          namespace: 'shodan-internetdb',
          keys: ['203.0.113.1'],
          lease_ttl_seconds: 30,
        },
      },
    }, 'You are not allowed to manage freshness for this connector', 'FORBIDDEN_ACCESS');
  });

  it('should reject out-of-bounds key sets at the GraphQL boundary', async () => {
    await queryAsUserIsExpectedError(USER_CONNECTOR, {
      query: ACQUIRE_FRESHNESS_QUERY,
      variables: {
        input: {
          connector_id: connectorId,
          namespace: 'shodan-internetdb',
          keys: [],
          lease_ttl_seconds: 30,
        },
      },
    }, 'Freshness keys must contain between 1 and 100 values', 'VALIDATION_ERROR');
  });

  it('should require connector capability for freshness mutations', async () => {
    await queryAsUserIsExpectedForbidden(USER_EDITOR, {
      query: ACQUIRE_FRESHNESS_QUERY,
      variables: {
        input: {
          connector_id: connectorId,
          namespace: 'shodan-internetdb',
          keys: ['203.0.113.2'],
          lease_ttl_seconds: 30,
        },
      },
    });
  });
});

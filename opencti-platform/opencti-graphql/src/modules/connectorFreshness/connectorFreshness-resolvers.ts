import type { Resolvers } from '../../generated/graphql';
import { acquireConnectorFreshness, completeConnectorFreshness, releaseConnectorFreshness } from './connectorFreshness-domain';

const connectorFreshnessResolvers: Resolvers = {
  Mutation: {
    connectorFreshnessAcquire: (_, { input }, context) => acquireConnectorFreshness(context, context.user, input),
    connectorFreshnessComplete: (_, { input }, context) => completeConnectorFreshness(context, context.user, input),
    connectorFreshnessRelease: (_, { input }, context) => releaseConnectorFreshness(context, context.user, input),
  },
};

export default connectorFreshnessResolvers;

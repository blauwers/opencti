import { registerGraphqlSchema } from '../../graphql/schema';
import connectorFreshnessResolvers from './connectorFreshness-resolvers';
import connectorFreshnessTypeDefs from './connectorFreshness.graphql';

registerGraphqlSchema({
  schema: connectorFreshnessTypeDefs,
  resolver: connectorFreshnessResolvers,
});

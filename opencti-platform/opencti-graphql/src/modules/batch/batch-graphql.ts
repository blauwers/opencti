import { registerGraphqlSchema } from '../../graphql/schema';
import batchResolvers from './batch-resolvers';
import batchTypeDefs from './batch.graphql';

registerGraphqlSchema({
  schema: batchTypeDefs,
  resolver: batchResolvers,
});

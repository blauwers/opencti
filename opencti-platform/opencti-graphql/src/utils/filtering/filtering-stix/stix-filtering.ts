import { FILTER_KEY_TESTERS_MAP } from './stix-testers';
import { type FilterEventContext, testFilterGroup } from '../boolean-logic-engine';
import { isUserCanAccessStixElement, SYSTEM_USER } from '../../access';
import type { AuthContext, AuthUser } from '../../../types/user';
import { getEntitiesMapFromCache } from '../../../database/cache';
import type { StixObject } from '../../../types/stix-2-1-common';
import { ENTITY_TYPE_RESOLVED_FILTERS } from '../../../schema/stixDomainObject';
import { type FilterGroup } from '../../../generated/graphql';
import type { FilterResolutionMap } from '../filtering-resolution';
import { buildResolutionMapForFilterGroup, resolveFilterGroup } from '../filtering-resolution';
import { UnsupportedError } from '../../../config/errors';
import { checkFiltersFormat } from '../filtering-utils';

// ----------------------------------------------------------------------------------------------------------------------

/**
 * check a FilterGroup's keys validity in stix filtering
 */
const checkFiltersKeysForStixMatch = (filterGroup: FilterGroup) => {
  filterGroup.filters.forEach((filter) => {
    if (!Array.isArray(filter.key)) {
      throw UnsupportedError('The provided filter key is not an array', { key: JSON.stringify(filter.key) });
    }
    if (filter.key.length !== 1) {
      throw UnsupportedError('Stix filtering can only be executed on a unique filter key', { key: JSON.stringify(filter.key) });
    }
    if (FILTER_KEY_TESTERS_MAP[filter.key[0]] === undefined) {
      const availableFilters = JSON.stringify(Object.keys(FILTER_KEY_TESTERS_MAP));
      throw UnsupportedError('Stix filtering is not compatible with the provided filter key', { key: JSON.stringify(filter.key), availableFilters });
    }
  });
  filterGroup.filterGroups.forEach((fg) => checkFiltersKeysForStixMatch(fg));
};

/**
 * validate a FilterGroup in stix filtering: check the filters format and the filter keys validity
 */
export const validateFilterGroupForStixMatch = (filterGroup: FilterGroup) => {
  // check filters format
  checkFiltersFormat(filterGroup);
  // check filters keys validity
  checkFiltersKeysForStixMatch(filterGroup);
};

// ----------------------------------------------------------------------------------------------------------------------
// STIX MATCH

export type PreparedStixFilterMatchFn = (
  stix: any,
  eventContext?: FilterEventContext,
) => Promise<boolean>;

const buildPreparedStixMatchFilterGroup = (
  context: AuthContext,
  user: AuthUser,
  filterGroup: FilterGroup | undefined,
  resolutionMap: FilterResolutionMap,
): PreparedStixFilterMatchFn => {
  let resolvedFilterGroupPromise: Promise<FilterGroup> | undefined;

  return async (stix: any, eventContext?: FilterEventContext): Promise<boolean> => {
    // first check: user access right to the element (according to markings, organization, etc.)
    const isUserHasAccessToElement = await isUserCanAccessStixElement(context, user, stix);
    if (!isUserHasAccessToElement) {
      return false;
    }

    // if no filters and the user has access: the stix always match
    if (!filterGroup) return true;

    // replace the ids in values if necessary, to adapt to the stix format
    if (!resolvedFilterGroupPromise) {
      resolvedFilterGroupPromise = resolveFilterGroup(context, user, filterGroup, resolutionMap);
    }
    const resolvedFilterGroup = await resolvedFilterGroupPromise;

    // then call our boolean engine on the filter group using the stix testers
    return testFilterGroup(stix, resolvedFilterGroup, FILTER_KEY_TESTERS_MAP, eventContext);
  };
};

/**
 * Middleware function that allow us to make unit tests by mocking the resolution map.
 * This is necessary because the map is built thanks to the cache, not available in unit tests.
 */
export const isStixMatchFilterGroup_MockableForUnitTests = async (
  context: AuthContext,
  user: AuthUser,
  stix: any,
  filterGroup: FilterGroup | undefined,
  resolutionMap: FilterResolutionMap,
  eventContext?: FilterEventContext,
): Promise<boolean> => {
  // we are limited to certain filter keys right now, so better throw an explicit error if a key is not compatible
  // Note that similar check is done when saving a filter in stream, taxii, feed, or playbook node.
  // This check should thus not fail here, theoretically.
  if (filterGroup) validateFilterGroupForStixMatch(filterGroup);

  const preparedMatcher = buildPreparedStixMatchFilterGroup(context, user, filterGroup, resolutionMap);
  return preparedMatcher(stix, eventContext);
};

/**
 * Prepare the request-scoped work needed to match one filter group against multiple stix objects.
 *
 * The returned matcher still checks access rights for each stix object. Filter resolution is delayed
 * until the first accessible object so callers that never evaluate an object keep the existing
 * short-circuit behavior.
 */
export const prepareStixMatchFilterGroup = async (
  context: AuthContext,
  user: AuthUser,
  filterGroup?: FilterGroup,
): Promise<PreparedStixFilterMatchFn> => {
  // resolve some of the ids as we filter on their corresponding values or standard-id for instance
  // the provided map will contain replacements for filter values, if any necessary.
  // we use the entities stored in cache for the "Resolved-Filters" (all the entities used by the saved filters - stream, trigger, playbooks)
  // see cacheManager.ts:platformResolvedFilters
  const cache = await getEntitiesMapFromCache<StixObject>(context, SYSTEM_USER, ENTITY_TYPE_RESOLVED_FILTERS);
  const map = filterGroup ? await buildResolutionMapForFilterGroup(context, user, filterGroup, cache) : new Map();

  // we are limited to certain filter keys right now, so better throw an explicit error if a key is not compatible
  // Note that similar check is done when saving a filter in stream, taxii, feed, or playbook node.
  // This check should thus not fail here, theoretically.
  if (filterGroup) validateFilterGroupForStixMatch(filterGroup);

  return buildPreparedStixMatchFilterGroup(context, user, filterGroup, map);
};

/**
 * Tells if a stix object matches a filter group given a certain context.
 * The input filter group is a stored filter (streams, triggers, playbooks), the stix object comes from the raw stream.
 *
 * This function will first check the user access rights to the stix object, then resolve parts of the filter groups if necessary,
 * prior to actually comparing the filter values with the stix values.
 * @param context
 * @param user
 * @param stix stix object from the raw event stream
 * @param filterGroup
 * @param eventContext optional event context for has_changed/not_has_changed operator evaluation
 * @throws {Error} on invalid filter keys
 */
export const isStixMatchFilterGroup = async (
  context: AuthContext,
  user: AuthUser,
  stix: any,
  filterGroup?: FilterGroup,
  eventContext?: FilterEventContext,
): Promise<boolean> => {
  const preparedMatcher = await prepareStixMatchFilterGroup(context, user, filterGroup);
  return preparedMatcher(stix, eventContext);
};

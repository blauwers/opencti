import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';
import { describe, expect, it } from 'vitest';

type Registration = {
  kind?: string;
  sealDescriptor?: string;
};

const readRegistrations = (relativePath: string): Registration[] => {
  const sourcePath = fileURLToPath(new URL(`../../../../src/${relativePath}`, import.meta.url));
  const sourceText = readFileSync(sourcePath, 'utf8');
  const sourceFile = ts.createSourceFile(sourcePath, sourceText, ts.ScriptTarget.Latest, true);
  const registrations: Registration[] = [];

  const readPropertyText = (objectLiteral: ts.ObjectLiteralExpression, propertyName: string): string | undefined => {
    const property = objectLiteral.properties.find((candidate): candidate is ts.PropertyAssignment => {
      if (!ts.isPropertyAssignment(candidate)) {
        return false;
      }
      return ts.isIdentifier(candidate.name) && candidate.name.text === propertyName;
    });
    return property?.initializer.getText(sourceFile);
  };

  const visit = (node: ts.Node) => {
    if (ts.isObjectLiteralExpression(node)) {
      const kind = readPropertyText(node, 'kind');
      if (kind?.startsWith('BatchSideEffectKind.')) {
        registrations.push({
          kind,
          sealDescriptor: readPropertyText(node, 'sealDescriptor'),
        });
      }
    }
    ts.forEachChild(node, visit);
  };

  visit(sourceFile);
  return registrations;
};

const descriptorNames = (registrations: Registration[], kind: string) => registrations
  .filter((registration) => registration.kind === kind)
  .map((registration) => registration.sealDescriptor);

describe('batch side effect seal registration descriptors', () => {
  const streamRegistrations = readRegistrations('database/stream/stream-handler.ts');
  const workRegistrations = readRegistrations('domain/work.js');
  const rabbitmqRegistrations = readRegistrations('database/rabbitmq.js');
  const middlewareRegistrations = readRegistrations('database/middleware.ts');
  const engineRegistrations = readRegistrations('database/engine.ts');
  const deleteOperationRegistrations = readRegistrations('modules/deleteOperation/deleteOperation-domain.ts');

  it('maps reviewed leaf registrations to narrow descriptor contracts', () => {
    expect(descriptorNames(streamRegistrations, 'BatchSideEffectKind.StreamPublication')).toEqual([
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.streamPublicationKeyedCoalesced',
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.streamPublicationRaw',
    ]);
    expect(descriptorNames(workRegistrations, 'BatchSideEffectKind.WorkLifecycle')).toEqual([
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.workLifecycleRedisInitialize',
    ]);
    expect(descriptorNames(rabbitmqRegistrations, 'BatchSideEffectKind.ConnectorDispatch')).toEqual([
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.connectorDispatchWorkerSend',
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.connectorDispatchConnectorSend',
    ]);
    expect(descriptorNames(middlewareRegistrations, 'BatchSideEffectKind.FileLifecycle')).toEqual([
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.fileLifecycleMoveAllFiles',
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.fileLifecycleDeleteAllObjectFiles',
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.fileLifecycleMarkRemoved',
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.fileLifecycleDeleteAllObjectFiles',
    ]);
    expect(descriptorNames(deleteOperationRegistrations, 'BatchSideEffectKind.FileLifecycle')).toEqual([
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.fileLifecycleMarkRestored',
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.fileLifecycleDeleteAllObjectFiles',
    ]);
    expect(descriptorNames(middlewareRegistrations, 'BatchSideEffectKind.AutoEnrichment')).toEqual([
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.autoEnrichmentUpdateEntity',
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.autoEnrichmentCreateEntity',
      'BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.autoEnrichmentUpdateEntity',
    ]);
  });

  it('keeps compatibility projections unclassified until they are split', () => {
    const compatibilityRegistrations = [...engineRegistrations, ...middlewareRegistrations]
      .filter((registration) => registration.kind === 'BatchSideEffectKind.CompatibilityProjection');

    expect(compatibilityRegistrations.length).toBeGreaterThan(0);
    expect(compatibilityRegistrations.every((registration) => registration.sealDescriptor === undefined)).toBe(true);
  });
});

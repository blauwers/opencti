import React, { FunctionComponent } from 'react';
import { FormikConfig } from 'formik';
import { RecordSourceSelectorProxy } from 'relay-runtime';
import Drawer, { DrawerControlledDialProps } from '@components/common/drawer/Drawer';
import { formatEmailsForApi } from '@components/settings/dissemination_lists/DisseminationListUtils';
import { graphql } from 'react-relay';
import { DisseminationListsLinesPaginationQuery$variables } from '@components/settings/dissemination_lists/__generated__/DisseminationListsLinesPaginationQuery.graphql';
import DisseminationListForm from '@components/settings/dissemination_lists/DisseminationListForm';
import { useFormatter } from '../../../../components/i18n';
import { insertNode } from '../../../../utils/store';
import { handleErrorInForm } from '../../../../relay/environment';
import useApiMutation from '../../../../utils/hooks/useApiMutation';
import CreateEntityControlledDial from '../../../../components/CreateEntityControlledDial';

export interface DisseminationListCreationFormData {
  name: string;
  emails: string;
  description: string;
}

const disseminationListCreationMutation = graphql`
  mutation DisseminationListCreationAddMutation($input: DisseminationListAddInput!) {
    disseminationListAdd(input: $input) {
      ...DisseminationListsLine_node
    }
  }
`;

const CreateDisseminationListControlledDial = (
  props: DrawerControlledDialProps,
) => (
  <CreateEntityControlledDial
    entityType='DisseminationList'
    {...props}
  />
);

interface DisseminationListCreationProps {
  paginationOptions: DisseminationListsLinesPaginationQuery$variables;
}

const DisseminationListCreation: FunctionComponent<DisseminationListCreationProps> = ({
  paginationOptions,
}) => {
  const { t_i18n } = useFormatter();
  const updater = (store: RecordSourceSelectorProxy, rootField: string) => {
    insertNode(
      store,
      'Pagination_disseminationLists',
      paginationOptions,
      rootField,
    );
  };

  const [commit] = useApiMutation(disseminationListCreationMutation);

  const onSubmit: FormikConfig<DisseminationListCreationFormData>['onSubmit'] = (
    values,
    { setSubmitting, resetForm, setErrors },
  ) => {
    const input = {
      name: values.name,
      emails: formatEmailsForApi(values.emails),
      description: values.description,
    };
    commit({
      variables: {
        input,
      },
      updater: (store) => {
        updater(store, 'disseminationListAdd');
      },
      onCompleted: () => {
        setSubmitting(false);
        resetForm();
      },
      onError: (error: Error) => {
        handleErrorInForm(error, setErrors);
        setSubmitting(false);
      },
    });
  };

  return (
    <Drawer
      title={t_i18n('Create a dissemination list')}
      controlledDial={CreateDisseminationListControlledDial}
    >
      {({ onClose }) => (
        <DisseminationListForm
          onSubmit={onSubmit}
          onReset={onClose}
        />
      )}
    </Drawer>
  );
};

export default DisseminationListCreation;

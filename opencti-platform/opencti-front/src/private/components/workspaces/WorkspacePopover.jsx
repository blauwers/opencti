import React, { useState } from 'react';
import Menu from '@mui/material/Menu';
import MenuItem from '@mui/material/MenuItem';
import IconButton from '@mui/material/IconButton';
import Box from '@mui/material/Box';
import MoreVert from '@mui/icons-material/MoreVert';
import makeStyles from '@mui/styles/makeStyles';
import PropTypes from 'prop-types';
import { useNavigate } from 'react-router-dom';
import PublicDashboardCreationForm from './dashboards/public_dashboards/PublicDashboardCreationForm';
import Drawer from '../common/drawer/Drawer';
import { useFormatter } from '../../../components/i18n';
import { QueryRenderer } from '../../../relay/environment';
import WorkspaceEditionContainer from './WorkspaceEditionContainer';
import Security from '../../../utils/Security';
import { EXPLORE_EXUPDATE, EXPLORE_EXUPDATE_EXDELETE, EXPLORE_EXUPDATE_PUBLISH, INVESTIGATION_INUPDATE_INDELETE } from '../../../utils/hooks/useGranted';
import { deleteNode, insertNode } from '../../../utils/store';
import handleExportJson from './workspaceExportHandler';
import WorkspaceDuplicationDialog from './WorkspaceDuplicationDialog';
import useApiMutation from '../../../utils/hooks/useApiMutation';
import { useGetCurrentUserAccessRight } from '../../../utils/authorizedMembers';
import stopEvent from '../../../utils/domEvent';
import DeleteDialog from '../../../components/DeleteDialog';
import useDeletion from '../../../utils/hooks/useDeletion';
import WorkspaceEditionQuery from './WorkspacePopoverContainerQuery';
import WorkspacePopoverDeletionMutation from './WorkspacePopoverDeletionMutation';

// Deprecated - https://mui.com/system/styles/basics/
// Do not use it for new code.
const useStyles = makeStyles(() => ({
  container: {
    margin: 0,
  },
}));

const WorkspacePopover = ({ workspace, paginationOptions }) => {
  const { id, type } = workspace;
  const navigate = useNavigate();
  const classes = useStyles();
  const { t_i18n } = useFormatter();

  const [anchorEl, setAnchorEl] = useState(null);
  const [displayEdit, setDisplayEdit] = useState(false);
  const [displayDuplicate, setDisplayDuplicate] = useState(false);
  const [duplicating, setDuplicating] = useState(false);

  const handleOpen = (event) => {
    stopEvent(event);
    setAnchorEl(event.currentTarget);
  };

  const handleClose = (event) => {
    stopEvent(event);
    setAnchorEl(null);
  };

  const handleCloseDuplicate = (event) => {
    if (event) stopEvent(event);
    setDisplayDuplicate(false);
  };

  const [commit] = useApiMutation(WorkspacePopoverDeletionMutation);

  const updater = (store) => {
    if (paginationOptions) {
      insertNode(store, 'Pagination_workspaces', paginationOptions, 'workspaceDuplicate');
    }
  };

  const deletion = useDeletion({ handleClose: () => setAnchorEl(null) });
  const { setDeleting, handleOpenDelete, handleCloseDelete } = deletion;

  const submitDelete = (event) => {
    stopEvent(event);
    setDeleting(true);
    commit({
      variables: { id },
      updater: (store) => {
        if (paginationOptions) {
          deleteNode(store, 'Pagination_workspaces', paginationOptions, id);
        }
      },
      onCompleted: () => {
        setDeleting(false);
        handleClose(event);
        if (paginationOptions) {
          handleCloseDelete(event);
        } else {
          navigate(`/dashboard/workspaces/${type}s`);
        }
      },
    });
  };

  const handleOpenEdit = (event) => {
    setDisplayEdit(true);
    handleClose(event);
  };

  const handleDashboardDuplication = (event) => {
    setDisplayDuplicate(true);
    handleClose(event);
  };

  const handleCloseEdit = () => setDisplayEdit(false);

  const { canManage, canEdit } = useGetCurrentUserAccessRight(workspace.currentUserAccessRight);
  if (!canEdit && workspace.type !== 'dashboard') {
    return <></>;
  }

  const goToPublicDashboards = (event) => {
    stopEvent(event);

    const filter = {
      mode: 'and',
      filterGroups: [],
      filters: [{
        key: 'dashboard_id',
        values: [workspace.id],
        mode: 'or',
        operator: 'eq',
      }],
    };
    navigate(`/dashboard/workspaces/dashboards_public?filters=${JSON.stringify(filter)}`);
  };

  // -- Creation public dashboard --
  const [displayCreate, setDisplayCreate] = useState(false);

  const handleOpenCreation = (event) => {
    setDisplayCreate(true);
    handleClose(event);
  };

  const handleCloseCreate = () => {
    setDisplayCreate(false);
  };

  const handleExport = (event) => {
    stopEvent(event);
    handleExportJson(workspace);
  };

  return (
    <div className={classes.container}>
      <IconButton
        onClick={handleOpen}
        aria-haspopup="true"
        size="small"
        color="primary"
        aria-label={t_i18n('Workspace popover of actions')}
      >
        <MoreVert />
      </IconButton>
      <Menu anchorEl={anchorEl} open={Boolean(anchorEl)} onClose={handleClose} aria-label="Workspace menu">
        <Security needs={[EXPLORE_EXUPDATE]} hasAccess={canEdit}>
          <MenuItem onClick={handleOpenEdit}>{t_i18n('Update')}</MenuItem>
        </Security>
        {workspace.type === 'dashboard' && (
          <Box>
            <Security needs={[EXPLORE_EXUPDATE]} hasAccess={canEdit}>
              <MenuItem onClick={handleDashboardDuplication}>{t_i18n('Duplicate')}</MenuItem>
            </Security>
            <Security needs={[EXPLORE_EXUPDATE]} hasAccess={canEdit}>
              <MenuItem onClick={handleExport}>{t_i18n('Export')}</MenuItem>
            </Security>
            <Security needs={[EXPLORE_EXUPDATE_EXDELETE]} hasAccess={canManage}>
              <MenuItem onClick={handleOpenDelete}>{t_i18n('Delete')}</MenuItem>
            </Security>
            <Box>
              <MenuItem onClick={goToPublicDashboards}>
                {t_i18n('View associated public dashboards')}
              </MenuItem>
              <Security needs={[EXPLORE_EXUPDATE_PUBLISH]} hasAccess={canManage}>
                <MenuItem onClick={handleOpenCreation}>{t_i18n('Create a public dashboard')}</MenuItem>
              </Security>
            </Box>
          </Box>
        )}
        {workspace.type === 'investigation' && (
          <Security needs={[INVESTIGATION_INUPDATE_INDELETE]} hasAccess={canManage}>
            <MenuItem onClick={handleOpenDelete}>{t_i18n('Delete')}</MenuItem>
          </Security>
        )}
      </Menu>
      <Drawer
        title={t_i18n('Create a public dashboard')}
        open={displayCreate}
        onClose={handleCloseCreate}
      >
        {({ onClose }) => (
          <PublicDashboardCreationForm
            onClose={handleCloseCreate}
            onCompleted={onClose}
            dashboard_id={workspace.id || undefined}
          />
        )}
      </Drawer>
      <WorkspaceDuplicationDialog
        workspace={workspace}
        displayDuplicate={displayDuplicate}
        handleCloseDuplicate={handleCloseDuplicate}
        duplicating={duplicating}
        setDuplicating={setDuplicating}
        updater={updater}
        paginationOptions={paginationOptions}
      />
      <DeleteDialog
        deletion={deletion}
        submitDelete={submitDelete}
        message={workspace.type === 'investigation'
          ? t_i18n('Do you want to delete this investigation?')
          : t_i18n('Do you want to delete this dashboard?')}
      />
      <QueryRenderer
        query={WorkspaceEditionQuery}
        variables={{ id }}
        render={({ props: editionProps }) => {
          if (!editionProps) {
            return <div />;
          }
          return (
            <WorkspaceEditionContainer
              workspace={editionProps.workspace}
              handleClose={handleCloseEdit}
              open={displayEdit}
              type={type}
            />
          );
        }}
      />
    </div>
  );
};

WorkspacePopover.propTypes = {
  workspace: PropTypes.object,
  paginationOptions: PropTypes.object,
};

export default WorkspacePopover;

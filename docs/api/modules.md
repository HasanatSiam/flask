# API Modules

This document provides a detailed reference for the API modules.

## Table of Contents
- [Users](#users)
- [RBAC](#rbac)
- [Aggregation](#aggregation)
- [Access Models](#access-models)
- [Access Points](#access-points)
- [Action Items](#action-items)
- [Asynchronous Tasks](#asynchronous-tasks)
- [Controls](#controls)
- [Data Sources](#data-sources)
- [Global Conditions](#global-conditions)
- [Messages](#messages)
- [Tenant Enterprises](#tenant-enterprises)

---

## Users

### Authentication & Credentials
-   **POST** `/users/login`: Authenticate user and retrieve tokens.
-   **POST** `/users/def_user_credentials`: Define user credentials.
-   **PUT** `/users/reset_user_password`: Reset user password.
-   **DELETE** `/users/def_user_credentials/<int:user_id>`: Delete user credentials.
-   **POST** `/users/create_request`: Create a password reset request.
-   **POST** `/users/reset_forgot_password`: Reset password using forgot password flow.
-   **GET** `/users/verify_request`: Verify a password reset request.

### User Management
-   **POST** `/users/users`: Create a new user.
-   **GET** `/users/users`: List all users.
-   **GET** `/users/users/<int:user_id>`: Get specific user details.
-   **PUT** `/users/users/<int:user_id>`: Update user details.
-   **DELETE** `/users/users/<int:user_id>`: Delete a user.

### Invitations
-   **POST** `/users/invitations/via_email`: Send invitation via email.
-   **POST** `/users/invitations/via_link`: Generate invitation link.
-   **GET** `/users/invitations/<string:encrypted_id>/<string:token>`: Verify invitation token.
-   **POST** `/users/invitations/accept/<encrypted_id>/<token>`: Accept invitation.

### Definitions (DefUsers / DefPersons)
-   **POST** `/users/defusers`: Create definition user.
-   **GET** `/users/defusers`: List definition users.
-   **GET** `/users/defusers/<int:page>/<int:limit>`: Paginated list of definition users.
-   **GET** `/users/defusers/search/<int:page>/<int:limit>`: Search definition users.
-   **GET** `/users/defusers/<int:user_id>`: Get definition user.
-   **PUT** `/users/defusers/<int:user_id>`: Update definition user.
-   **DELETE** `/users/defusers/<int:user_id>`: Delete definition user.
-   **POST** `/users/defpersons`: Create person definition.
-   **GET** `/users/defpersons`: List person definitions.
-   **GET** `/users/defpersons/<int:page>/<int:limit>`: Paginated list of persons.
-   **GET** `/users/defpersons/search/<int:page>/<int:limit>`: Search persons.
-   **GET** `/users/defpersons/<int:user_id>`: Get person details.
-   **PUT** `/users/defpersons/<int:user_id>`: Update person details.
-   **DELETE** `/users/defpersons/<int:user_id>`: Delete person.

### Access Profiles
-   **POST** `/users/access_profiles/<int:user_id>`: Create access profile for user.
-   **GET** `/users/access_profiles`: List all access profiles.
-   **GET** `/users/access_profiles/<int:user_id>`: Get access profiles for a user.
-   **PUT** `/users/access_profiles/<int:user_id>/<int:serial_number>`: Update access profile.
-   **DELETE** `/users/access_profiles/<int:user_id>/<int:serial_number>`: Delete access profile.

---

## RBAC

### Roles & Privileges
-   **POST** `/rbac/def_roles`: Define a new role.
-   **GET** `/rbac/def_roles`: List all roles.
-   **PUT** `/rbac/def_roles`: Update a role.
-   **DELETE** `/rbac/def_roles`: Delete a role.
-   **POST** `/rbac/def_privileges`: Define a new privilege.
-   **GET** `/rbac/def_privileges`: List all privileges.
-   **PUT** `/rbac/def_privileges`: Update a privilege.
-   **DELETE** `/rbac/def_privileges`: Delete a privilege.

### User Assignments
-   **POST** `/rbac/def_user_granted_roles`: Grant role to user.
-   **GET** `/rbac/def_user_granted_roles`: List user granted roles.
-   **PUT** `/rbac/def_user_granted_roles`: Update user granted role.
-   **DELETE** `/rbac/def_user_granted_roles`: Revoke role from user.
-   **POST** `/rbac/def_user_granted_privileges`: Grant privilege to user.
-   **GET** `/rbac/def_user_granted_privileges`: List user granted privileges.
-   **PUT** `/rbac/def_user_granted_privileges`: Update user granted privilege.
-   **DELETE** `/rbac/def_user_granted_privileges`: Revoke privilege from user.

### API Endpoint Security
-   **POST** `/rbac/def_api_endpoints`: Define API endpoint.
-   **GET** `/rbac/def_api_endpoints`: List API endpoints.
-   **PUT** `/rbac/def_api_endpoints`: Update API endpoint.
-   **DELETE** `/rbac/def_api_endpoints`: Delete API endpoint.
-   **POST** `/rbac/def_api_endpoint_roles`: Assign role to API endpoint.
-   **GET** `/rbac/def_api_endpoint_roles`: List API endpoint roles.
-   **PUT** `/rbac/def_api_endpoint_roles`: Update API endpoint role.
-   **DELETE** `/rbac/def_api_endpoint_roles`: Remove role from API endpoint.

---

## Aggregation

-   **POST** `/aggregation/create_aggregate_table`: Create an aggregation table.
---

## Access Models

### Logic Attributes
-   **POST** `/access_models/def_access_model_logic_attributes`: Create logic attribute.
-   **GET** `/access_models/def_access_model_logic_attributes`: List logic attributes.
-   **POST** `/access_models/def_access_model_logic_attributes/upsert`: Upsert logic attribute.
-   **PUT** `/access_models/def_access_model_logic_attributes`: Update logic attribute.
-   **DELETE** `/access_models/def_access_model_logic_attributes`: Delete logic attribute.

### Logics
-   **POST** `/access_models/def_access_model_logics`: Create logic.
-   **POST** `/access_models/def_access_model_logics/upsert`: Upsert logic.
-   **GET** `/access_models/def_access_model_logics`: List logics.
-   **PUT** `/access_models/def_access_model_logics`: Update logic.
-   **DELETE** `/access_models/def_access_model_logics`: Delete logic.

### Models
-   **POST** `/access_models/def_access_models`: Create access model.
-   **GET** `/access_models/def_access_models`: List access models.
-   **PUT** `/access_models/def_access_models`: Update access model.
-   **DELETE** `/access_models/def_access_models`: Delete access model.
-   **DELETE** `/access_models/def_access_models/cascade`: Cascade delete access model.

---

## Access Points

### Definitions
-   **POST** `/access_points/def_access_points`: Create access point.
-   **GET** `/access_points/def_access_points`: List access points.
-   **GET** `/access_points/def_access_points_view`: View access points.
-   **PUT** `/access_points/def_access_points`: Update access point.
-   **DELETE** `/access_points/def_access_points`: Delete access point.

### Entitlements
-   **POST** `/access_points/def_access_entitlement_elements`: Create entitlement element.
-   **GET** `/access_points/def_access_entitlement_elements`: List entitlement elements.
-   **DELETE** `/access_points/def_access_entitlement_elements`: Delete entitlement element.
-   **GET** `/access_points/def_access_entitlements`: List entitlements.
-   **GET** `/access_points/def_access_entitlements/<int:page>/<int:limit>`: Paginated entitlements.
-   **POST** `/access_points/def_access_entitlements`: Create entitlement.
-   **PUT** `/access_points/def_access_entitlements`: Update entitlement.
-   **DELETE** `/access_points/def_access_entitlements`: Delete entitlement.
-   **DELETE** `/access_points/def_access_entitlements/cascade`: Cascade delete entitlement.

---

## Action Items

### Assignments
-   **POST** `/action_items/def_action_item_assignments`: Assign action item.
-   **GET** `/action_items/def_action_item_assignments`: List assignments.
-   **DELETE** `/action_items/def_action_item_assignments/<int:user_id>/<int:action_item_id>`: Delete assignment.

### Items
-   **POST** `/action_items/def_action_items`: Create action item.
-   **GET** `/action_items/def_action_items`: List action items.
-   **GET** `/action_items/def_action_items/<int:page>/<int:limit>`: Paginated action items.
-   **GET** `/action_items/def_action_items/<int:action_item_id>`: Get action item details.
-   **POST** `/action_items/def_action_items/upsert`: Upsert action item.
-   **PUT** `/action_items/def_action_items/<int:action_item_id>`: Update action item.
-   **DELETE** `/action_items/def_action_items/<int:action_item_id>`: Delete action item.
-   **PUT** `/action_items/def_action_items/update_status/<int:user_id>/<int:action_item_id>`: Update status.
-   **GET** `/action_items/def_action_items_view/<int:user_id>/<int:page>/<int:limit>`: View user action items.
---

## Asynchronous Tasks

### View Requests
-   **GET** `/async_task/view_requests_v1`: View requests (v1).
-   **GET** `/async_task/view_requests_v2`: View requests (v2).
-   **GET** `/async_task/view_requests/<int:page>/<int:page_limit>`: Paginated requests.
-   **GET** `/async_task/view_requests/search/<int:page>/<int:limit>`: Search requests.
-   **GET** `/async_task/view_requests_v3/<int:page>/<int:limit>`: View requests (v3).
-   **GET** `/async_task/view_requests_v4/<int:page>/<int:limit>`: View requests (v4).

### Task Schedules
-   **POST** `/async_task/Create_TaskSchedule`: Create task schedule.
-   **GET** `/async_task/Show_TaskSchedules`: List task schedules.
-   **GET** `/async_task/def_async_task_schedules/<int:page>/<int:limit>`: Paginated task schedules.
-   **GET** `/async_task/def_async_task_schedules/search/<int:page>/<int:limit>`: Search task schedules.
-   **GET** `/async_task/Show_TaskSchedule/<string:task_name>`: Get task schedule details.
-   **PUT** `/async_task/Update_TaskSchedule/<string:task_name>`: Update task schedule.
-   **PUT** `/async_task/Cancel_TaskSchedule/<string:task_name>`: Cancel task schedule.
-   **PUT** `/async_task/Reschedule_Task/<string:task_name>`: Reschedule task.
-   **PUT** `/async_task/Cancel_AdHoc_Task/...`: Cancel ad-hoc task.

### Task Parameters
-   **POST** `/async_task/Add_TaskParams/<string:task_name>`: Add task parameters.
-   **GET** `/async_task/Show_TaskParams/<string:task_name>`: Show task parameters.
-   **GET** `/async_task/Show_TaskParams/<string:task_name>/<int:page>/<int:limit>`: Paginated task parameters.
-   **PUT** `/async_task/Update_TaskParams/<string:task_name>/<int:def_param_id>`: Update task parameters.
-   **DELETE** `/async_task/Delete_TaskParams/<string:task_name>/<int:def_param_id>`: Delete task parameters.

### Tasks
-   **POST** `/async_task/Create_Task`: Create task.
-   **GET** `/async_task/def_async_tasks`: List tasks.
-   **GET** `/async_task/def_async_tasks/v1`: List tasks (v1).
-   **GET** `/async_task/def_async_tasks/<int:page>/<int:limit>`: Paginated tasks.
-   **GET** `/async_task/def_async_tasks/search/<int:page>/<int:limit>`: Search tasks.
-   **GET** `/async_task/Show_Task/<task_name>`: Get task details.
-   **PUT** `/async_task/Update_Task/<string:task_name>`: Update task.
-   **PUT** `/async_task/Cancel_Task/<string:task_name>`: Cancel task.

### Execution Methods
-   **POST** `/async_task/Create_ExecutionMethod`: Create execution method.
-   **GET** `/async_task/Show_ExecutionMethods`: List execution methods.
-   **GET** `/async_task/Show_ExecutionMethods/v1`: List execution methods (v1).
-   **GET** `/async_task/Show_ExecutionMethods/<int:page>/<int:limit>`: Paginated execution methods.
-   **GET** `/async_task/def_async_execution_methods/search/<int:page>/<int:limit>`: Search execution methods.
-   **GET** `/async_task/Show_ExecutionMethod/<string:internal_execution_method>`: Get execution method details.
-   **PUT** `/async_task/Update_ExecutionMethod/<string:internal_execution_method>`: Update execution method.
-   **DELETE** `/async_task/Delete_ExecutionMethod/<string:internal_execution_method>`: Delete execution method.

---

## Controls

### Environments
-   **POST** `/controls/def_control_environments`: Create control environment.
-   **GET** `/controls/def_control_environments`: List control environments.
-   **PUT** `/controls/def_control_environments`: Update control environment.
-   **DELETE** `/controls/def_control_environments`: Delete control environment.

### Controls
-   **POST** `/controls/def_controls`: Create control.
-   **GET** `/controls/def_controls`: List controls.
-   **PUT** `/controls/def_controls`: Update control.
-   **DELETE** `/controls/def_controls`: Delete control.

---

## Data Sources

-   **POST** `/data_sources/def_data_sources`: Create data source.
-   **GET** `/data_sources/def_data_sources`: List data sources.
-   **PUT** `/data_sources/def_data_sources`: Update data source.
---

## Global Conditions

### Logic Attributes
-   **POST** `/global_conditions/def_global_condition_logic_attributes`: Create logic attribute.
-   **GET** `/global_conditions/def_global_condition_logic_attributes`: List logic attributes.
-   **GET** `/global_conditions/def_global_condition_logic_attributes/<int:page>/<int:limit>`: Paginated logic attributes.
-   **POST** `/global_conditions/def_global_condition_logic_attributes/upsert`: Upsert logic attribute.
-   **PUT** `/global_conditions/def_global_condition_logic_attributes`: Update logic attribute.
-   **DELETE** `/global_conditions/def_global_condition_logic_attributes`: Delete logic attribute.

### Logics
-   **POST** `/global_conditions/def_global_condition_logics`: Create logic.
-   **POST** `/global_conditions/def_global_condition_logics/upsert`: Upsert logic.
-   **GET** `/global_conditions/def_global_condition_logics`: List logics.
-   **PUT** `/global_conditions/def_global_condition_logics`: Update logic.
-   **DELETE** `/global_conditions/def_global_condition_logics`: Delete logic.

### Conditions
-   **POST** `/global_conditions/def_global_conditions`: Create global condition.
-   **GET** `/global_conditions/def_global_conditions`: List global conditions.
-   **PUT** `/global_conditions/def_global_conditions`: Update global condition.
-   **DELETE** `/global_conditions/def_global_conditions`: Delete global condition.
-   **DELETE** `/global_conditions/def_global_conditions/cascade`: Cascade delete global condition.

---

## Messages

-   **GET** `/messages/messages`: List messages.
-   **POST** `/messages/messages`: Create message.
-   **GET** `/messages/messages/<string:id>`: Get message details.
-   **PUT** `/messages/messages/<string:id>`: Update message.
-   **DELETE** `/messages/messages/<string:id>`: Delete message.

---

## Tenant Enterprises

### Tenants
-   **POST** `/tenant_enterprises/def_tenants`: Create tenant.
-   **GET** `/tenant_enterprises/def_tenants`: List tenants.
-   **GET** `/tenant_enterprises/tenants/v1`: List tenants (v1).
-   **GET** `/tenant_enterprises/def_tenants/<int:page>/<int:limit>`: Paginated tenants.
-   **GET** `/tenant_enterprises/def_tenants/search/<int:page>/<int:limit>`: Search tenants.
-   **GET** `/tenant_enterprises/tenants/<int:tenant_id>`: Get tenant details.
-   **PUT** `/tenant_enterprises/def_tenants`: Update tenant.
-   **DELETE** `/tenant_enterprises/def_tenants`: Delete tenant.
-   **DELETE** `/tenant_enterprises/tenants/cascade_delete`: Cascade delete tenant.

### Job Titles
-   **POST** `/tenant_enterprises/job_titles`: Create job title.
-   **GET** `/tenant_enterprises/job_titles`: List job titles.
-   **PUT** `/tenant_enterprises/job_titles`: Update job title.
-   **DELETE** `/tenant_enterprises/job_titles`: Delete job title.

### Enterprises
-   **POST** `/tenant_enterprises/def_tenant_enterprise_setup`: Setup enterprise.
-   **POST** `/tenant_enterprises/create_enterpriseV1/<int:tenant_id>`: Create enterprise (v1).
-   **GET** `/tenant_enterprises/get_enterprises`: List enterprises.
-   **GET** `/tenant_enterprises/get_enterprises/v1`: List enterprises (v1).
-   **GET** `/tenant_enterprises/enterprises`: List enterprises (alias).
-   **GET** `/tenant_enterprises/def_tenant_enterprise_setup`: Get enterprise setup.
-   **GET** `/tenant_enterprises/def_tenant_enterprise_setup/<int:page>/<int:limit>`: Paginated enterprise setup.
-   **GET** `/tenant_enterprises/def_tenant_enterprise_setup/search/<int:page>/<int:limit>`: Search enterprise setup.
-   **PUT** `/tenant_enterprises/update_enterprise/<int:tenant_id>`: Update enterprise.
-   **DELETE** `/tenant_enterprises/def_tenant_enterprise_setup`: Delete enterprise setup.

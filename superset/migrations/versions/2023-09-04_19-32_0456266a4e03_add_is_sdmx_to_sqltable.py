# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""add is_sdmx to sqltable

Revision ID: 0456266a4e03
Revises: ec54aca4c8a2
Create Date: 2023-09-04 19:32:10.949475

"""

# revision identifiers, used by Alembic.
revision = '0456266a4e03'
down_revision = 'ec54aca4c8a2'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('annotation', 'layer_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.alter_column('dashboard_roles', 'dashboard_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.alter_column('dbs', 'allow_file_upload',
               existing_type=sa.BOOLEAN(),
               nullable=True,
               existing_server_default=sa.text('true'))
    op.create_unique_constraint(None, 'dynamic_plugin', ['bundle_url'])
    op.alter_column('embedded_dashboards', 'uuid',
               existing_type=postgresql.UUID(),
               nullable=False)
    op.create_foreign_key(None, 'embedded_dashboards', 'ab_user', ['changed_by_fk'], ['id'])
    op.create_foreign_key(None, 'embedded_dashboards', 'ab_user', ['created_by_fk'], ['id'])
    op.alter_column('filter_sets', 'dashboard_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.create_unique_constraint(None, 'filter_sets', ['name'])
    op.drop_index('ix_key_value_expires_on', table_name='key_value')
    op.drop_index('ix_key_value_uuid', table_name='key_value')
    op.create_unique_constraint(None, 'key_value', ['uuid'])
    op.drop_index('ix_logs_user_id_dttm', table_name='logs')
    op.alter_column('report_schedule', 'extra_json',
               existing_type=sa.TEXT(),
               nullable=True)
    op.drop_index('ix_creation_method', table_name='report_schedule')
    op.create_unique_constraint(None, 'report_schedule_user', ['user_id', 'report_schedule_id'])
    op.drop_index('ix_row_level_security_filters_filter_type', table_name='row_level_security_filters')
    op.alter_column('sl_columns', 'is_additive',
               existing_type=sa.BOOLEAN(),
               nullable=True)
    op.alter_column('sl_columns', 'is_aggregation',
               existing_type=sa.BOOLEAN(),
               nullable=True)
    op.alter_column('sl_columns', 'is_increase_desired',
               existing_type=sa.BOOLEAN(),
               nullable=True)
    op.alter_column('sl_columns', 'is_partition',
               existing_type=sa.BOOLEAN(),
               nullable=True)
    op.alter_column('sl_columns', 'is_physical',
               existing_type=sa.BOOLEAN(),
               nullable=True)
    op.alter_column('sl_columns', 'is_spatial',
               existing_type=sa.BOOLEAN(),
               nullable=True)
    op.alter_column('sl_columns', 'is_temporal',
               existing_type=sa.BOOLEAN(),
               nullable=True)
    op.drop_constraint('sl_table_columns_table_id_fkey', 'sl_table_columns', type_='foreignkey')
    op.drop_constraint('sl_table_columns_column_id_fkey', 'sl_table_columns', type_='foreignkey')
    op.create_foreign_key(None, 'sl_table_columns', 'sl_tables', ['table_id'], ['id'], ondelete='CASCADE')
    op.create_foreign_key(None, 'sl_table_columns', 'sl_columns', ['column_id'], ['id'], ondelete='CASCADE')
    op.create_unique_constraint(None, 'sl_tables', ['database_id', 'catalog', 'schema', 'name'])
    op.alter_column('ssh_tunnels', 'database_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.drop_index('ix_ssh_tunnels_database_id', table_name='ssh_tunnels')
    op.drop_index('ix_ssh_tunnels_uuid', table_name='ssh_tunnels')
    op.create_unique_constraint(None, 'ssh_tunnels', ['uuid'])
    op.create_unique_constraint(None, 'ssh_tunnels', ['database_id'])
    op.create_foreign_key(None, 'ssh_tunnels', 'ab_user', ['changed_by_fk'], ['id'])
    op.create_foreign_key(None, 'ssh_tunnels', 'ab_user', ['created_by_fk'], ['id'])
    op.alter_column('tab_state', 'autorun',
               existing_type=sa.BOOLEAN(),
               nullable=True)
    op.alter_column('tab_state', 'hide_left_bar',
               existing_type=sa.BOOLEAN(),
               nullable=True,
               existing_server_default=sa.text('false'))
    op.drop_index('ix_tab_state_id', table_name='tab_state')
    op.drop_index('ix_table_schema_id', table_name='table_schema')
    op.add_column('tables', sa.Column('is_sdmx', sa.Boolean(), nullable=True))
    op.drop_index('ix_tagged_object_object_id', table_name='tagged_object')
    op.create_foreign_key(None, 'tagged_object', 'dashboards', ['object_id'], ['id'])
    op.create_foreign_key(None, 'tagged_object', 'saved_query', ['object_id'], ['id'])
    op.create_foreign_key(None, 'tagged_object', 'slices', ['object_id'], ['id'])
    op.alter_column('user_favorite_tag', 'user_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('user_favorite_tag', 'tag_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('user_favorite_tag', 'tag_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.alter_column('user_favorite_tag', 'user_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.drop_constraint(None, 'tagged_object', type_='foreignkey')
    op.drop_constraint(None, 'tagged_object', type_='foreignkey')
    op.drop_constraint(None, 'tagged_object', type_='foreignkey')
    op.create_index('ix_tagged_object_object_id', 'tagged_object', ['object_id'], unique=False)
    op.drop_column('tables', 'is_sdmx')
    op.create_index('ix_table_schema_id', 'table_schema', ['id'], unique=False)
    op.create_index('ix_tab_state_id', 'tab_state', ['id'], unique=False)
    op.alter_column('tab_state', 'hide_left_bar',
               existing_type=sa.BOOLEAN(),
               nullable=False,
               existing_server_default=sa.text('false'))
    op.alter_column('tab_state', 'autorun',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    op.drop_constraint(None, 'ssh_tunnels', type_='foreignkey')
    op.drop_constraint(None, 'ssh_tunnels', type_='foreignkey')
    op.drop_constraint(None, 'ssh_tunnels', type_='unique')
    op.drop_constraint(None, 'ssh_tunnels', type_='unique')
    op.create_index('ix_ssh_tunnels_uuid', 'ssh_tunnels', ['uuid'], unique=False)
    op.create_index('ix_ssh_tunnels_database_id', 'ssh_tunnels', ['database_id'], unique=False)
    op.alter_column('ssh_tunnels', 'database_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.drop_constraint(None, 'sl_tables', type_='unique')
    op.drop_constraint(None, 'sl_table_columns', type_='foreignkey')
    op.drop_constraint(None, 'sl_table_columns', type_='foreignkey')
    op.create_foreign_key('sl_table_columns_column_id_fkey', 'sl_table_columns', 'sl_columns', ['column_id'], ['id'])
    op.create_foreign_key('sl_table_columns_table_id_fkey', 'sl_table_columns', 'sl_tables', ['table_id'], ['id'])
    op.alter_column('sl_columns', 'is_temporal',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    op.alter_column('sl_columns', 'is_spatial',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    op.alter_column('sl_columns', 'is_physical',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    op.alter_column('sl_columns', 'is_partition',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    op.alter_column('sl_columns', 'is_increase_desired',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    op.alter_column('sl_columns', 'is_aggregation',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    op.alter_column('sl_columns', 'is_additive',
               existing_type=sa.BOOLEAN(),
               nullable=False)
    op.create_index('ix_row_level_security_filters_filter_type', 'row_level_security_filters', ['filter_type'], unique=False)
    op.drop_constraint(None, 'report_schedule_user', type_='unique')
    op.create_index('ix_creation_method', 'report_schedule', ['creation_method'], unique=False)
    op.alter_column('report_schedule', 'extra_json',
               existing_type=sa.TEXT(),
               nullable=False)
    op.create_index('ix_logs_user_id_dttm', 'logs', ['user_id', 'dttm'], unique=False)
    op.drop_constraint(None, 'key_value', type_='unique')
    op.create_index('ix_key_value_uuid', 'key_value', ['uuid'], unique=False)
    op.create_index('ix_key_value_expires_on', 'key_value', ['expires_on'], unique=False)
    op.drop_constraint(None, 'filter_sets', type_='unique')
    op.alter_column('filter_sets', 'dashboard_id',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.drop_constraint(None, 'embedded_dashboards', type_='foreignkey')
    op.drop_constraint(None, 'embedded_dashboards', type_='foreignkey')
    op.alter_column('embedded_dashboards', 'uuid',
               existing_type=postgresql.UUID(),
               nullable=True)
    op.drop_constraint(None, 'dynamic_plugin', type_='unique')
    op.alter_column('dbs', 'allow_file_upload',
               existing_type=sa.BOOLEAN(),
               nullable=False,
               existing_server_default=sa.text('true'))
    op.alter_column('dashboard_roles', 'dashboard_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('annotation', 'layer_id',
               existing_type=sa.INTEGER(),
               nullable=True)
    # ### end Alembic commands ###
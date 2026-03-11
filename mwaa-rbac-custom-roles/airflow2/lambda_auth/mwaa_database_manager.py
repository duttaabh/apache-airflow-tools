"""
MWAA Database Manager - Helper class for MWAA metadata database operations

This module provides a clean interface for managing users and roles in the
MWAA metadata database (PostgreSQL).

Key Features:
- Context manager for automatic connection and transaction management
- User creation and updates
- Role assignment and management
- Proper error handling and logging
"""

import logging

logger = logging.getLogger()


class MWAADatabaseManager:
    """
    Helper class to manage MWAA metadata database operations.
    
    This class handles:
    - Database connection management
    - User creation and updates
    - Role assignment
    - Transaction management
    
    Usage:
        with MWAADatabaseManager(host, port, db, user, password) as db:
            db.update_user_role(username='john.doe', role_name='Admin')
    """
    
    def __init__(self, db_host, db_port, db_name, db_username, db_password):
        """
        Initialize the database manager.
        
        Args:
            db_host: Database host
            db_port: Database port
            db_name: Database name
            db_username: Database username
            db_password: Database password
        """
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_username = db_username
        self.db_password = db_password
        self.conn = None
        self.cursor = None
    
    def __enter__(self):
        """Context manager entry - establish database connection."""
        try:
            import psycopg2
            
            logger.info(f'Connecting to database: {self.db_host}:{self.db_port}/{self.db_name}')
            
            self.conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_username,
                password=self.db_password,
                connect_timeout=10
            )
            self.cursor = self.conn.cursor()
            logger.info('✓ Database connection established')
            return self
            
        except Exception as e:
            logger.error(f'Failed to connect to database: {e}')
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if exc_type is None:
                self.conn.commit()
                logger.info('✓ Transaction committed')
            else:
                self.conn.rollback()
                logger.error('✗ Transaction rolled back due to error')
            self.conn.close()
            logger.info('Database connection closed')
    
    def get_or_create_user(self, username, email=None, first_name='', last_name=''):
        """
        Get existing user or create a new one.
        
        Args:
            username: Username (e.g., 'assumed-role/RoleName')
            email: User email (optional)
            first_name: User first name (optional)
            last_name: User last name (optional)
        
        Returns:
            int: User ID
        """
        # Check if user exists
        self.cursor.execute(
            "SELECT id FROM ab_user WHERE username = %s",
            (username,)
        )
        result = self.cursor.fetchone()
        
        if result:
            user_id = result[0]
            logger.info(f'✓ User "{username}" already exists with ID: {user_id}')
            
            # Update user details if provided
            if email or first_name or last_name:
                update_fields = []
                update_values = []
                
                if email:
                    update_fields.append("email = %s")
                    update_values.append(email)
                if first_name:
                    update_fields.append("first_name = %s")
                    update_values.append(first_name)
                if last_name:
                    update_fields.append("last_name = %s")
                    update_values.append(last_name)
                
                if update_fields:
                    update_values.append(user_id)
                    update_query = f"UPDATE ab_user SET {', '.join(update_fields)} WHERE id = %s"
                    self.cursor.execute(update_query, tuple(update_values))
                    logger.info(f'✓ Updated user details for "{username}"')
            
            return user_id
        
        # Create new user
        logger.info(f'Creating new user: {username}')
        
        # Use email as username if not provided
        if not email:
            email = f'{username}@example.com'
        
        self.cursor.execute(
            """
            INSERT INTO ab_user (username, email, first_name, last_name, active, created_on, changed_on)
            VALUES (%s, %s, %s, %s, true, NOW(), NOW())
            RETURNING id
            """,
            (username, email, first_name, last_name)
        )
        
        user_id = self.cursor.fetchone()[0]
        logger.info(f'✓ Created new user "{username}" with ID: {user_id}')
        
        return user_id
    
    def get_role_id(self, role_name):
        """
        Get role ID by role name.
        
        Args:
            role_name: Name of the role (e.g., 'Admin', 'User', 'MWAARestrictedTest')
        
        Returns:
            int: Role ID or None if not found
        """
        self.cursor.execute(
            "SELECT id FROM ab_role WHERE name = %s",
            (role_name,)
        )
        result = self.cursor.fetchone()
        
        if result:
            role_id = result[0]
            logger.info(f'✓ Found role "{role_name}" with ID: {role_id}')
            return role_id
        else:
            logger.warning(f'✗ Role "{role_name}" not found in database')
            return None
    
    def remove_all_user_roles(self, user_id):
        """
        Remove all roles from a user.
        
        Args:
            user_id: User ID
        """
        self.cursor.execute(
            "DELETE FROM ab_user_role WHERE user_id = %s",
            (user_id,)
        )
        deleted_count = self.cursor.rowcount
        logger.info(f'✓ Removed {deleted_count} role(s) from user ID: {user_id}')
    
    def assign_role_to_user(self, user_id, role_id):
        """
        Assign a role to a user.
        
        Args:
            user_id: User ID
            role_id: Role ID
        """
        # Check if assignment already exists
        self.cursor.execute(
            "SELECT 1 FROM ab_user_role WHERE user_id = %s AND role_id = %s",
            (user_id, role_id)
        )
        
        if self.cursor.fetchone():
            logger.info(f'✓ User {user_id} already has role {role_id}')
            return
        
        # Create new assignment
        self.cursor.execute(
            "INSERT INTO ab_user_role (user_id, role_id) VALUES (%s, %s)",
            (user_id, role_id)
        )
        logger.info(f'✓ Assigned role {role_id} to user {user_id}')
    
    def update_user_role(self, username, role_name, email=None, first_name=None, last_name=None):
        """
        Update user role - removes all existing roles and assigns the specified role.
        
        This is the main method that combines all operations:
        1. Get or create user
        2. Remove all existing roles
        3. Assign the new role
        
        Args:
            username: Username
            role_name: Role name to assign
            email: User email (optional)
            first_name: User first name (optional, defaults to username)
            last_name: User last name (optional, defaults to username)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Default first_name and last_name to username if not provided
            if first_name is None:
                first_name = username
            if last_name is None:
                last_name = username
            
            # Get or create user
            user_id = self.get_or_create_user(
                username, 
                email=email, 
                first_name=first_name, 
                last_name=last_name
            )
            
            # Get role ID
            role_id = self.get_role_id(role_name)
            if not role_id:
                logger.error(f'✗ Cannot assign non-existent role: {role_name}')
                return False
            
            # Remove all existing roles
            self.remove_all_user_roles(user_id)
            
            # Assign the new role
            self.assign_role_to_user(user_id, role_id)
            
            logger.info(f'✓ Successfully updated user "{username}" with role "{role_name}"')
            return True
            
        except Exception as e:
            logger.error(f'✗ Error updating user role: {e}')
            import traceback
            logger.error(f'Traceback: {traceback.format_exc()}')
            return False
    
    def get_user_roles(self, username):
        """
        Get all roles assigned to a user.
        
        Args:
            username: Username
        
        Returns:
            list: List of role names
        """
        self.cursor.execute(
            """
            SELECT r.name
            FROM ab_user u
            JOIN ab_user_role ur ON u.id = ur.user_id
            JOIN ab_role r ON ur.role_id = r.id
            WHERE u.username = %s
            """,
            (username,)
        )
        
        roles = [row[0] for row in self.cursor.fetchall()]
        logger.info(f'User "{username}" has roles: {roles}')
        return roles
    
    def list_all_roles(self):
        """
        List all available roles in the system.
        
        Returns:
            list: List of tuples (role_id, role_name)
        """
        self.cursor.execute("SELECT id, name FROM ab_role ORDER BY name")
        roles = self.cursor.fetchall()
        logger.info(f'Found {len(roles)} roles in database')
        return roles
    
    def user_exists(self, username):
        """
        Check if a user exists.
        
        Args:
            username: Username to check
        
        Returns:
            bool: True if user exists, False otherwise
        """
        self.cursor.execute(
            "SELECT 1 FROM ab_user WHERE username = %s",
            (username,)
        )
        exists = self.cursor.fetchone() is not None
        logger.info(f'User "{username}" exists: {exists}')
        return exists

    def role_exists(self, role_name):
        """
        Check if a role exists.
        
        Args:
            role_name: Role name to check
        
        Returns:
            bool: True if role exists, False otherwise
        """
        self.cursor.execute(
            "SELECT 1 FROM ab_role WHERE name = %s",
            (role_name,)
        )
        exists = self.cursor.fetchone() is not None
        logger.info(f'Role "{role_name}" exists: {exists}')
        return exists
    
    def create_custom_role(self, role_name, source_role='User', specific_dags=None):
        """
        Create a custom MWAA role with specific DAG permissions.
        
        This method:
        1. Copies all non-DAG permissions from the source role
        2. Adds permissions for specific DAGs if provided
        3. Adds "menu access on DAGs" permission
        
        Args:
            role_name: Name of the custom role to create
            source_role: Base role to copy permissions from (default: 'User')
            specific_dags: List of DAG names to grant access to (e.g., ['dag1', 'dag2'])
                          If None or empty, no DAG-specific permissions are added
        
        Returns:
            bool: True if role created successfully, False otherwise
        """
        try:
            logger.info(f'Creating custom role: {role_name} from {source_role}')
            logger.info(f'Specific DAGs: {specific_dags}')
            
            # Step 1: Check if role already exists
            if self.role_exists(role_name):
                logger.info(f'Role "{role_name}" already exists, skipping creation')
                return True
            
            # Step 2: Get source role ID
            self.cursor.execute("SELECT id FROM ab_role WHERE name = %s", (source_role,))
            source_role_data = self.cursor.fetchone()
            
            if not source_role_data:
                logger.error(f'Source role "{source_role}" not found')
                return False
            
            source_role_id = source_role_data[0]
            logger.info(f'✓ Found source role "{source_role}" with ID: {source_role_id}')
            
            # Step 3: Get permissions from source role (excluding wildcard DAG permissions)
            self.cursor.execute(
                """
                SELECT pv.id as permission_view_id
                FROM ab_role r
                JOIN ab_permission_view_role pvr ON r.id = pvr.role_id
                JOIN ab_permission_view pv ON pvr.permission_view_id = pv.id
                JOIN ab_view_menu vm ON pv.view_menu_id = vm.id
                JOIN ab_permission p ON pv.permission_id = p.id
                WHERE r.id = %s 
                  AND vm.name NOT LIKE 'DAG:%%'
                  AND vm.name NOT LIKE 'DAGs%%'
                """,
                (source_role_id,)
            )
            
            non_dag_perms = self.cursor.fetchall()
            permission_view_ids = [perm[0] for perm in non_dag_perms]
            
            logger.info(f'✓ Found {len(permission_view_ids)} non-DAG permissions from source role')
            
            # Step 4: Add DAG-specific permissions if specified
            if specific_dags:
                logger.info(f'Adding permissions for specific DAGs: {specific_dags}')
                
                # Get permissions for specific DAGs
                dag_names = [f'DAG:{dag}' for dag in specific_dags]
                placeholders = ','.join(['%s'] * len(dag_names))
                
                self.cursor.execute(
                    f"""
                    SELECT pv.id as permission_view_id
                    FROM ab_permission_view pv
                    JOIN ab_view_menu vm ON pv.view_menu_id = vm.id
                    WHERE vm.name IN ({placeholders})
                    """,
                    tuple(dag_names)
                )
                
                dag_perms = self.cursor.fetchall()
                dag_perm_ids = [perm[0] for perm in dag_perms]
                permission_view_ids.extend(dag_perm_ids)
                
                logger.info(f'✓ Added {len(dag_perm_ids)} DAG-specific permissions')
                
                # Add "menu access on DAGs" permission
                self.cursor.execute(
                    """
                    SELECT pv.id as permission_view_id
                    FROM ab_permission_view pv
                    JOIN ab_view_menu vm ON pv.view_menu_id = vm.id
                    JOIN ab_permission p ON pv.permission_id = p.id
                    WHERE vm.name = 'DAGs' AND p.name like 'menu%'
                    """
                )
                
                dags_menu_perm = self.cursor.fetchone()
                if dags_menu_perm:
                    permission_view_ids.append(dags_menu_perm[0])
                    logger.info('✓ Added DAGs menu access permission')
            
            logger.info(f'Total permissions to assign: {len(permission_view_ids)}')
            
            # Step 5: Get next role ID
            self.cursor.execute("SELECT MAX(id) as max_id FROM ab_role")
            max_id_result = self.cursor.fetchone()
            max_id = max_id_result[0] if max_id_result[0] else 0
            new_role_id = max_id + 1
            
            logger.info(f'New role ID: {new_role_id}')
            
            # Step 6: Create the new role
            self.cursor.execute(
                "INSERT INTO ab_role (id, name) VALUES (%s, %s)",
                (new_role_id, role_name)
            )
            logger.info(f'✓ Created role "{role_name}" with ID: {new_role_id}')
            
            # Step 7: Assign permissions to the new role
            for pv_id in permission_view_ids:
                self.cursor.execute(
                    "INSERT INTO ab_permission_view_role (permission_view_id, role_id) VALUES (%s, %s)",
                    (pv_id, new_role_id)
                )
            
            logger.info(f'✓ Assigned {len(permission_view_ids)} permissions to role "{role_name}"')
            logger.info(f'✓ Successfully created custom role "{role_name}"')
            
            return True
            
        except Exception as e:
            logger.error(f'Error creating custom role: {e}')
            import traceback
            logger.error(f'Traceback: {traceback.format_exc()}')
            return False

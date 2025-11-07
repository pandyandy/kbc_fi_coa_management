"""
COA-specific data management utilities
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import streamlit as st
from datetime import datetime
import json
import os
import uuid

try:
    # Optional dependency: only required when reading/writing to Keboola
    from keboola_streamlit import KeboolaStreamlit  # type: ignore
except Exception:
    KeboolaStreamlit = None  # type: ignore

class COADataManager:
    """Manages COA (Chart of Accounts) data operations"""
    
    def __init__(self):
        self.data = None
        self.business_units = []
        self.audit_log = []
        self.data_file_path = "dev_data/38624723.in.c_keboola_ex_google_drive_01k7haj8zpdqrevsrchqxx4p87.kbc_coa_input.csv"
        # Track original snapshot when loaded from Keboola
        self.original_data: Optional[pd.DataFrame] = None
        # Prepare session-scoped delta tracking
        if 'session_id' not in st.session_state:
            st.session_state['session_id'] = str(uuid.uuid4())
        self.session_id: str = st.session_state['session_id']
        self.session_changes: pd.DataFrame = pd.DataFrame(columns=[
            'action',
            'timestamp',
            'PK_BUSINESS_SUBUNIT',
            'NUM_FIN_STAT_ORDER',
            'CODE_FIN_STAT',
            'NAME_FIN_STAT',
            'CODE_PARENT_FIN_STAT',
            'TYPE_ACCOUNT',
            'TYPE_FIN_STATEMENT',
            'NAME_FIN_STAT_ENG'
        ])
        self.session_changes_file = os.path.join(
            os.getcwd(), 'dev_data', f"session_changes_{self.session_id}.csv"
        )
        
    def load_coa_data(self, file_path: str = None) -> pd.DataFrame:
        """Load COA data from Keboola only (no CSV fallback)."""
        try:
            if not self._can_use_keboola():
                raise RuntimeError("Keboola credentials not configured. Please set 'kbc_url' and 'kbc_token' in .streamlit/secrets.toml")
            
            df = self._read_from_keboola("out.c-002_consolidation_coa.DC_COA")
            self.original_data = df.copy()
            
            # Standardize column names
            df.columns = df.columns.str.upper()
            
            # Ensure PK business subunit column exists
            if 'PK_BUSINESS_SUBUNIT' not in df.columns:
                df['PK_BUSINESS_SUBUNIT'] = None
            
            # Ensure required columns exist
            required_columns = [
                'NUM_FIN_STAT_ORDER', 'CODE_FIN_STAT', 'NAME_FIN_STAT', 
                'CODE_PARENT_FIN_STAT', 'TYPE_ACCOUNT', 'TYPE_FIN_STATEMENT',
                'NAME_FIN_STAT_ENG'
            ]
            
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Convert order to numeric
            df['NUM_FIN_STAT_ORDER'] = pd.to_numeric(df['NUM_FIN_STAT_ORDER'], errors='coerce')
            
            # Add hierarchy level
            df['HIERARCHY_LEVEL'] = self._calculate_hierarchy_levels(df)
            
            self.data = df
            self.business_units = df['PK_BUSINESS_SUBUNIT'].unique().tolist()
            
            return df
            
        except Exception as e:
            st.error(f"Error loading COA data: {str(e)}")
            return pd.DataFrame()

    def _can_use_keboola(self) -> bool:
        """Check if Keboola credentials and package are available."""
        try:
            return (
                KeboolaStreamlit is not None and
                'kbc_token' in st.secrets and
                'kbc_url' in st.secrets and
                bool(st.secrets['kbc_token']) and
                bool(st.secrets['kbc_url'])
            )
        except Exception:
            return False

    def _read_from_keboola(self, table_id: str) -> pd.DataFrame:
        """Read a table from Keboola Storage via keboola-streamlit client."""
        if KeboolaStreamlit is None:
            raise RuntimeError("keboola-streamlit package not installed")
        
        @st.cache_data(show_spinner=False)
        def _cached_read_table(root_url: str, token: str, table: str) -> pd.DataFrame:
            client = KeboolaStreamlit(root_url=root_url, token=token)
            return client.read_table(table_id=table)

        return _cached_read_table(st.secrets['kbc_url'], st.secrets['kbc_token'], table_id)
    
    def _calculate_hierarchy_levels(self, df: pd.DataFrame) -> List[int]:
        """Calculate hierarchy levels for each row"""
        levels = [0] * len(df)
        
        # Create parent-child mapping
        parent_map = {}
        for idx, row in df.iterrows():
            parent = row['CODE_PARENT_FIN_STAT']
            if pd.notna(parent) and parent != '':
                parent_map[row['CODE_FIN_STAT']] = parent
        
        # Calculate levels recursively
        def get_level(code):
            if code not in parent_map:
                return 0
            return 1 + get_level(parent_map[code])
        
        for idx, row in df.iterrows():
            levels[idx] = get_level(row['CODE_FIN_STAT'])
        
        return levels
    
    def get_business_units(self) -> List[str]:
        """Get list of available business units"""
        if self.data is not None:
            return self.data['PK_BUSINESS_SUBUNIT'].unique().tolist()
        return []
    
    def get_flat_data(self) -> pd.DataFrame:
        """Get flat representation of COA data"""
        if self.data is None:
            self.load_coa_data()
        return self.data.copy()
    
    def filter_by_business_unit(self, business_unit: str) -> pd.DataFrame:
        """Filter data by business unit"""
        if self.data is None:
            return pd.DataFrame()
        
        return self.data[self.data['PK_BUSINESS_SUBUNIT'] == business_unit].copy()
    
    def get_hierarchical_structure(self, business_unit: str = None, fin_statement: str = None) -> Dict[str, Any]:
        """Get hierarchical structure of COA data"""
        df = self.filter_by_business_unit(business_unit) if business_unit else self.data
        
        # Apply financial statement filter if provided
        if fin_statement and df is not None and not df.empty:
            df = df[df['TYPE_FIN_STATEMENT'] == fin_statement].copy()
        
        if df is None or df.empty:
            return {}
        
        # Build hierarchy starting from root items
        # Root items are those with CODE_PARENT_FIN_STAT matching the statement type (BS/PL)
        if fin_statement:
            # When filtering by statement type, find root items where CODE_PARENT_FIN_STAT equals the statement type
            root_items = df[df['CODE_PARENT_FIN_STAT'] == fin_statement]
        else:
            # When no statement filter, find all root items (both BS and PL)
            root_items = df[(df['CODE_PARENT_FIN_STAT'] == 'BS') | (df['CODE_PARENT_FIN_STAT'] == 'PL')]
        
        # Sort root items by NUM_FIN_STAT_ORDER
        root_items = root_items.sort_values('NUM_FIN_STAT_ORDER', na_position='last')
        
        hierarchy = {}
        
        for _, item in root_items.iterrows():
            hierarchy[item['CODE_FIN_STAT']] = {
                'data': item.to_dict(),
                'children': self._build_children_structure(df, item['CODE_FIN_STAT'])
            }
        
        return hierarchy
    
    def _build_children_structure(self, df: pd.DataFrame, parent_code: str) -> List[Dict]:
        """Recursively build children structure"""
        children = df[df['CODE_PARENT_FIN_STAT'] == parent_code]
        
        # Sort children by NUM_FIN_STAT_ORDER
        children = children.sort_values('NUM_FIN_STAT_ORDER', na_position='last')
        
        result = []
        
        for _, child in children.iterrows():
            child_data = {
                'data': child.to_dict(),
                'children': self._build_children_structure(df, child['CODE_FIN_STAT'])
            }
            result.append(child_data)
        
        return result
    
    def get_next_order_for_parent(self, parent_code: str, business_unit: str = None) -> int:
        """Get the next order value for a child under the given parent"""
        if self.data is None or self.data.empty:
            return 1000
        
        # Filter by business unit if provided
        df = self.data
        if business_unit:
            df = df[df['PK_BUSINESS_SUBUNIT'] == business_unit]
        
        # Find all children of the parent
        children = df[df['CODE_PARENT_FIN_STAT'] == parent_code]
        
        if children.empty:
            # If no children, start with 1000
            return 1000
        
        # Get the maximum order value and add 100
        max_order = children['NUM_FIN_STAT_ORDER'].max()
        if pd.isna(max_order):
            return 1000
        
        return int(max_order) + 100
    
    def search_coa(self, query: str, business_unit: str = None, 
                   type_account: str = None, type_fin_statement: str = None) -> pd.DataFrame:
        """Search COA data with filters"""
        df = self.filter_by_business_unit(business_unit) if business_unit else self.data
        
        if df is None or df.empty:
            return pd.DataFrame()
        
        # Text search
        if query:
            mask = (
                df['NAME_FIN_STAT'].str.contains(query, case=False, na=False) |
                df['CODE_FIN_STAT'].str.contains(query, case=False, na=False) |
                df['NAME_FIN_STAT_ENG'].str.contains(query, case=False, na=False)
            )
            df = df[mask]
        
        # Filter by account type
        if type_account:
            df = df[df['TYPE_ACCOUNT'] == type_account]
        
        # Filter by financial statement type
        if type_fin_statement:
            df = df[df['TYPE_FIN_STATEMENT'] == type_fin_statement]
        
        return df
    
    def validate_coa_rules(self, df: pd.DataFrame) -> List[str]:
        """Validate COA data against business rules"""
        errors = []
        
        # Rule 1: Balance Sheet accounts (A, P) should have TYPE_FIN_STATEMENT = 'BS'
        bs_violations = df[
            (df['TYPE_ACCOUNT'].isin(['A', 'P'])) & 
            (df['TYPE_FIN_STATEMENT'] != 'BS')
        ]
        if not bs_violations.empty:
            errors.append(f"Balance Sheet accounts (A, P) must have TYPE_FIN_STATEMENT = 'BS'. Found {len(bs_violations)} violations.")
        
        # Rule 2: Profit & Loss accounts (R, C) should have TYPE_FIN_STATEMENT = 'PL'
        pl_violations = df[
            (df['TYPE_ACCOUNT'].isin(['R', 'C'])) & 
            (df['TYPE_FIN_STATEMENT'] != 'PL')
        ]
        if not pl_violations.empty:
            errors.append(f"Profit & Loss accounts (R, C) must have TYPE_FIN_STATEMENT = 'PL'. Found {len(pl_violations)} violations.")
        
        # Rule 3: Check for duplicate codes within business subunit
        if 'PK_BUSINESS_SUBUNIT' in df.columns:
            duplicates = df.groupby(['PK_BUSINESS_SUBUNIT', 'CODE_FIN_STAT']).size()
            duplicates = duplicates[duplicates > 1]
            if not duplicates.empty:
                errors.append(f"Duplicate CODE_FIN_STAT found within business units: {duplicates.index.tolist()}")
        
        # Rule 4: Check for orphaned parent references
        all_codes = set(df['CODE_FIN_STAT'].dropna())
        parent_codes = set(df['CODE_PARENT_FIN_STAT'].dropna())
        orphaned_parents = parent_codes - all_codes
        if orphaned_parents:
            errors.append(f"Orphaned parent references found: {list(orphaned_parents)}")
        
        return errors
    
    def add_coa_item(self, item_data: Dict[str, Any], user: str = "system") -> bool:
        """Add new COA item with audit logging"""
        try:
            # Validate required fields
            required_fields = ['CODE_FIN_STAT', 'NAME_FIN_STAT', 'TYPE_ACCOUNT', 'TYPE_FIN_STATEMENT']
            for field in required_fields:
                if field not in item_data or not item_data[field]:
                    raise ValueError(f"Required field {field} is missing")
            
            # Check for duplicate codes within the same business unit
            if self.data is not None and not self.data.empty:
                business_unit = item_data.get('PK_BUSINESS_SUBUNIT', None)
                existing_codes = self.data[
                    (self.data['CODE_FIN_STAT'] == item_data['CODE_FIN_STAT']) & 
                    (self.data['PK_BUSINESS_SUBUNIT'] == business_unit)
                ]
                if not existing_codes.empty:
                    raise ValueError(f"Code '{item_data['CODE_FIN_STAT']}' already exists in business unit '{business_unit}'. Please use a unique code within this business unit.")
            
            # Validate business rules
            type_account = item_data['TYPE_ACCOUNT']
            type_statement = item_data['TYPE_FIN_STATEMENT']
            
            # Rule: BS accounts must have TYPE_ACCOUNT A or P
            if type_statement == 'BS' and type_account not in ['A', 'P']:
                raise ValueError("Balance Sheet accounts must have Account Type A (Assets) or P (Liabilities/Equity)")
            
            # Rule: PL accounts must have TYPE_ACCOUNT R or C
            if type_statement == 'PL' and type_account not in ['R', 'C']:
                raise ValueError("Profit & Loss accounts must have Account Type R (Revenue) or C (Cost)")
            
            # Validate parent code exists if provided
            if item_data.get('CODE_PARENT_FIN_STAT') and item_data['CODE_PARENT_FIN_STAT']:
                if self.data is not None and not self.data.empty:
                    if item_data['CODE_PARENT_FIN_STAT'] not in self.data['CODE_FIN_STAT'].values:
                        raise ValueError(f"Parent code '{item_data['CODE_PARENT_FIN_STAT']}' does not exist. Please use a valid parent code.")
            
            # Add to dataframe
            new_row = pd.DataFrame([item_data])
            self.data = pd.concat([self.data, new_row], ignore_index=True)
            
            # Update session state with the new data
            if 'coa_working_data' in st.session_state:
                st.session_state['coa_working_data'] = self.data.copy()
                st.session_state['has_unsaved_changes'] = True
            
            # For Keboola-backed sessions, record deltas to per-session CSV
            self._record_session_change('ADD', new_row.iloc[0].to_dict())
            
            # Log audit
            self._log_audit('ADD', item_data['CODE_FIN_STAT'], user, item_data)
            
            return True
            
        except Exception as e:
            st.error(f"Error adding COA item: {str(e)}")
            return False
    
    def update_coa_item(self, code: str, updates: Dict[str, Any], user: str = "system") -> bool:
        """Update COA item with audit logging"""
        try:
            # Find the item
            mask = self.data['CODE_FIN_STAT'] == code
            if not mask.any():
                raise ValueError(f"COA item with code {code} not found")
            
            # Store old values for audit
            old_values = self.data[mask].iloc[0].to_dict()
            
            # Check for duplicate codes within the same business unit (if code is being updated)
            if 'CODE_FIN_STAT' in updates:
                new_code = updates['CODE_FIN_STAT']
                business_unit = updates.get('PK_BUSINESS_SUBUNIT', old_values.get('PK_BUSINESS_SUBUNIT', None))
                
                # Check if the new code already exists in the same business unit (excluding current item)
                existing_codes = self.data[
                    (self.data['CODE_FIN_STAT'] == new_code) & 
                    (self.data['PK_BUSINESS_SUBUNIT'] == business_unit) &
                    (self.data['CODE_FIN_STAT'] != code)  # Exclude current item
                ]
                if not existing_codes.empty:
                    raise ValueError(f"Code '{new_code}' already exists in business unit '{business_unit}'. Please use a unique code within this business unit.")
            
            # Update the item
            for key, value in updates.items():
                if key in self.data.columns:
                    self.data.loc[mask, key] = value
            
            # Update session state with the modified data
            if 'coa_working_data' in st.session_state:
                st.session_state['coa_working_data'] = self.data.copy()
                st.session_state['has_unsaved_changes'] = True
            
            # Record delta for session
            updated_row = self.data[mask].iloc[0].to_dict()
            self._record_session_change('UPDATE', updated_row)
            
            # Log audit
            self._log_audit('UPDATE', code, user, updates, old_values)
            
            return True
            
        except Exception as e:
            st.error(f"Error updating COA item: {str(e)}")
            return False
    
    def delete_coa_item(self, code: str, user: str = "system") -> bool:
        """Delete COA item with audit logging"""
        try:
            # Check if item has children
            children = self.data[self.data['CODE_PARENT_FIN_STAT'] == code]
            if not children.empty:
                raise ValueError(f"Cannot delete item {code} - it has {len(children)} children")
            
            # Store old values for audit
            mask = self.data['CODE_FIN_STAT'] == code
            old_values = self.data[mask].iloc[0].to_dict()
            
            # Delete the item
            self.data = self.data[~mask]
            
            # Update session state with the modified data
            if 'coa_working_data' in st.session_state:
                st.session_state['coa_working_data'] = self.data.copy()
                st.session_state['has_unsaved_changes'] = True
            
            # Record delta for session
            self._record_session_change('DELETE', old_values)
            
            # Log audit
            self._log_audit('DELETE', code, user, {}, old_values)
            
            return True
            
        except Exception as e:
            st.error(f"Error deleting COA item: {str(e)}")
            return False
    
    def _log_audit(self, action: str, code: str, user: str, new_values: Dict, old_values: Dict = None):
        """Log audit trail"""
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'action': action,
            'code': code,
            'user': user,
            'new_values': new_values,
            'old_values': old_values
        }
        self.audit_log.append(audit_entry)
    
    def get_audit_log(self, code: str = None) -> List[Dict]:
        """Get audit log for specific code or all"""
        if code:
            return [entry for entry in self.audit_log if entry['code'] == code]
        return self.audit_log
    
    def save_coa_data(self, updated_df: pd.DataFrame = None, file_path: str = None) -> bool:
        """Save COA data to CSV file (dev fallback).

        When running against Keboola, edits are tracked only in the session delta CSV
        via _record_session_change and this method is not used to persist to master.
        """
        try:
            # If updated_df is provided, use it; otherwise use self.data
            df_to_save = updated_df if updated_df is not None else self.data
            
            if df_to_save is None or df_to_save.empty:
                st.error("No data to save")
                return False
            
            if file_path is None:
                file_path = self.data_file_path
            
            # Update internal data if we're saving an updated dataframe
            if updated_df is not None:
                self.data = updated_df.copy()
            
            # Save to CSV (excluding HIERARCHY_LEVEL as it's calculated)
            cols_to_save = [col for col in df_to_save.columns if col != 'HIERARCHY_LEVEL']
            df_to_save[cols_to_save].to_csv(file_path, index=False)
            
            return True
            
        except Exception as e:
            st.error(f"Error saving COA data: {str(e)}")
            return False

    def _record_session_change(self, action: str, row_data: Dict[str, Any]):
        """Append a change record to in-memory frame and to the session-specific CSV."""
        change = {
            'action': action,
            'timestamp': datetime.now().isoformat(),
            'PK_BUSINESS_SUBUNIT': row_data.get('PK_BUSINESS_SUBUNIT'),
            'NUM_FIN_STAT_ORDER': row_data.get('NUM_FIN_STAT_ORDER'),
            'CODE_FIN_STAT': row_data.get('CODE_FIN_STAT'),
            'NAME_FIN_STAT': row_data.get('NAME_FIN_STAT'),
            'CODE_PARENT_FIN_STAT': row_data.get('CODE_PARENT_FIN_STAT'),
            'TYPE_ACCOUNT': row_data.get('TYPE_ACCOUNT'),
            'TYPE_FIN_STATEMENT': row_data.get('TYPE_FIN_STATEMENT'),
            'NAME_FIN_STAT_ENG': row_data.get('NAME_FIN_STAT_ENG')
        }
        self.session_changes = pd.concat([self.session_changes, pd.DataFrame([change])], ignore_index=True)
        try:
            os.makedirs(os.path.dirname(self.session_changes_file), exist_ok=True)
            header = not os.path.exists(self.session_changes_file)
            pd.DataFrame([change]).to_csv(self.session_changes_file, index=False, mode='a', header=header)
        except Exception as e:
            st.error(f"Failed to write session changes CSV: {e}")
    
    def export_to_excel(self, business_unit: str = None) -> bytes:
        """Export COA data to Excel format"""
        df = self.filter_by_business_unit(business_unit) if business_unit else self.data
        
        if df is None or df.empty:
            return b''
        
        # Create Excel file in memory
        output = pd.ExcelWriter('coa_export.xlsx', engine='openpyxl')
        df.to_excel(output, sheet_name='COA_Data', index=False)
        output.close()
        
        # Read the file and return bytes
        with open('coa_export.xlsx', 'rb') as f:
            return f.read()
    
    def import_from_excel(self, file_content: bytes) -> bool:
        """Import COA data from Excel file"""
        try:
            # Save uploaded file temporarily
            with open('temp_import.xlsx', 'wb') as f:
                f.write(file_content)
            
            # Read Excel file
            df = pd.read_excel('temp_import.xlsx')
            
            # Validate and process
            errors = self.validate_coa_rules(df)
            if errors:
                st.error("Validation errors found:")
                for error in errors:
                    st.error(error)
                return False
            
            # Update data
            self.data = df
            self.business_units = df['PK_BUSINESS_SUBUNIT'].unique().tolist()
            
            return True
            
        except Exception as e:
            st.error(f"Error importing Excel file: {str(e)}")
            return False

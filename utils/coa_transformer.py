"""
COA Transformation Module
Replicates the SQL transformation logic for Chart of Accounts enrichment
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
import streamlit as st

try:
    from keboola_streamlit import KeboolaStreamlit
except Exception:
    KeboolaStreamlit = None


class COATransformer:
    """Handles COA data transformation and enrichment"""
    
    def __init__(self):
        self.coa_input = None
        self.business_subunits = None
        self.coa_output = None
        self.keboola_client = None
        self._init_keboola_client()
    
    def _init_keboola_client(self):
        """Initialize Keboola client if credentials are available"""
        try:
            if KeboolaStreamlit and hasattr(st, 'secrets'):
                kbc_url = st.secrets.get('keboola_url')
                kbc_token = st.secrets.get('keboola_token')
                if kbc_url and kbc_token:
                    self.keboola_client = KeboolaStreamlit(kbc_url, kbc_token)
        except Exception:
            pass
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def load_business_subunits_from_keboola(_self) -> pd.DataFrame:
        """Load business subunits from Keboola with caching"""
        if not _self.keboola_client:
            raise RuntimeError("Keboola client not initialized. Please check credentials in .streamlit/secrets.toml")
        
        try:
            # Read from Keboola table
            business_subunits = _self.keboola_client.read_table("out.c-999_initiation_tables_creation.DC_BUSINESS_SUBUNIT")
            _self.business_subunits = business_subunits
            return business_subunits
        except Exception as e:
            raise RuntimeError(f"Failed to load business subunits from Keboola: {str(e)}")
        
    def load_business_subunits(self, file_path: str) -> pd.DataFrame:
        """Load business subunits data"""
        try:
            self.business_subunits = pd.read_csv(file_path)
            return self.business_subunits
        except Exception as e:
            print(f"Error loading business subunits: {e}")
            return pd.DataFrame()
    
    def skey(self, id_value: int) -> str:
        """
        Creates string from order of the item - eg. from 1 to 01
        Replicates: substring('00' || id::varchar, -2)
        """
        return str(id_value).zfill(2)
    
    def transform_coa(self, coa_input_df: pd.DataFrame) -> pd.DataFrame:
        """
        Main transformation function that processes COA data
        """
        self.coa_input = coa_input_df.copy()
        
        # Step 1: Create COA_TEMPLATE_TEMP
        coa_temp = self._create_coa_template_temp()
        
        # Step 2: Build recursive hierarchy
        coa_hierarchy = self._build_hierarchy(coa_temp)
        
        # Step 3: Flatten hierarchy and create level columns
        coa_flattened = self._flatten_hierarchy(coa_hierarchy)
        
        # Step 4: Identify leaf nodes
        coa_with_leafs = self._identify_leafs(coa_flattened)
        
        self.coa_output = coa_with_leafs
        return coa_with_leafs
    
    def _create_coa_template_temp(self) -> pd.DataFrame:
        """
        Creates COA_TEMPLATE_TEMP with ranking
        """
        # Filter valid orders
        df = self.coa_input.copy()
        df['NUM_FIN_STAT_ORDER'] = pd.to_numeric(df['NUM_FIN_STAT_ORDER'], errors='coerce')
        df = df[df['NUM_FIN_STAT_ORDER'].notna()]
        
        # Create ranking within each group
        df['RANK'] = df.groupby(['TYPE_FIN_STATEMENT', 'CODE_PARENT_FIN_STAT'])['NUM_FIN_STAT_ORDER'].rank(method='first').astype(int)
        df['CODE_FIN_STAT_RANK'] = df['RANK'].apply(self.skey)
        
        # Select columns
        coa_temp = df[[
            'NUM_FIN_STAT_ORDER',
            'CODE_FIN_STAT_RANK',
            'CODE_FIN_STAT',
            'NAME_FIN_STAT',
            'CODE_PARENT_FIN_STAT',
            'TYPE_ACCOUNT',
            'TYPE_FIN_STATEMENT',
            'NAME_FIN_STAT_ENG'
        ]].copy()
        
        return coa_temp
    
    def _build_hierarchy(self, coa_temp: pd.DataFrame) -> pd.DataFrame:
        """
        Builds recursive hierarchy structure
        """
        # Start with root items (where CODE_PARENT_FIN_STAT is 'BS' or 'PL')
        hierarchy_list = []
        
        # Anchor: Root level items
        root_items = coa_temp[coa_temp['CODE_PARENT_FIN_STAT'].isin(['BS', 'PL'])].copy()
        root_items['NUM_FIN_STAT_LEVEL'] = 0
        root_items['INDENT'] = ''
        root_items['CODE_FIN_STAT_FULL'] = root_items['CODE_FIN_STAT']
        root_items['NAME_FIN_STAT_FULL'] = root_items['CODE_FIN_STAT_RANK'] + '-' + root_items['NAME_FIN_STAT']
        
        hierarchy_list.append(root_items)
        
        # Recursive: Build child levels
        current_level = root_items
        max_depth = 10  # Safety limit
        
        for level in range(1, max_depth + 1):
            # Find children of current level
            parent_data = current_level[['CODE_FIN_STAT', 'NUM_FIN_STAT_LEVEL', 'INDENT', 'CODE_FIN_STAT_FULL', 'NAME_FIN_STAT_FULL']].copy()
            parent_data.columns = ['PARENT_CODE', 'PARENT_LEVEL', 'PARENT_INDENT', 'PARENT_CODE_FULL', 'PARENT_NAME_FULL']
            
            children = coa_temp.merge(
                parent_data,
                left_on='CODE_PARENT_FIN_STAT',
                right_on='PARENT_CODE',
                how='inner'
            )
            
            if children.empty:
                break
            
            # Update hierarchy fields
            children['NUM_FIN_STAT_LEVEL'] = children['PARENT_LEVEL'] + 1
            children['INDENT'] = children['PARENT_INDENT'] + '--- '
            children['CODE_FIN_STAT_FULL'] = children['PARENT_CODE_FULL'] + ' | ' + children['CODE_FIN_STAT']
            children['NAME_FIN_STAT_FULL'] = children['PARENT_NAME_FULL'] + ' | ' + children['CODE_FIN_STAT_RANK'] + '-' + children['NAME_FIN_STAT']
            
            # Select relevant columns
            children = children[[
                'NUM_FIN_STAT_LEVEL',
                'NUM_FIN_STAT_ORDER',
                'CODE_FIN_STAT_RANK',
                'CODE_FIN_STAT',
                'NAME_FIN_STAT',
                'CODE_PARENT_FIN_STAT',
                'TYPE_ACCOUNT',
                'TYPE_FIN_STATEMENT',
                'NAME_FIN_STAT_ENG',
                'INDENT',
                'CODE_FIN_STAT_FULL',
                'NAME_FIN_STAT_FULL'
            ]]
            
            hierarchy_list.append(children)
            current_level = children
        
        # Combine all levels
        hierarchy_df = pd.concat(hierarchy_list, ignore_index=True)
        
        return hierarchy_df
    
    def _flatten_hierarchy(self, hierarchy_df: pd.DataFrame) -> pd.DataFrame:
        """
        Flattens hierarchy into L1-L10 columns
        """
        result = hierarchy_df.copy()
        
        # Add parent name
        parent_lookup = result[['CODE_FIN_STAT', 'NAME_FIN_STAT']].set_index('CODE_FIN_STAT')['NAME_FIN_STAT'].to_dict()
        result['NAME_FIN_STAT_PARENT'] = result['CODE_PARENT_FIN_STAT'].map(parent_lookup)
        result['NAME_FIN_STAT_INDENT'] = result['INDENT'] + result['NAME_FIN_STAT']
        
        # Split CODE_FIN_STAT_FULL and NAME_FIN_STAT_FULL into level columns
        for i in range(1, 11):  # L1 to L10
            level_idx = i - 1
            
            # CODE_FIN_STAT levels
            result[f'CODE_FIN_STAT_L{i}'] = result.apply(
                lambda row: self._get_level_value(row['CODE_FIN_STAT_FULL'], level_idx, row['NUM_FIN_STAT_LEVEL']),
                axis=1
            )
            
            # NAME_FIN_STAT levels
            result[f'NAME_FIN_STAT_L{i}'] = result.apply(
                lambda row: self._get_level_value(row['NAME_FIN_STAT_FULL'], level_idx, row['NUM_FIN_STAT_LEVEL']),
                axis=1
            )
            
            # NAME_FIN_STAT ordered name (remove first 3 chars which is the rank)
            result[f'NAME_FIN_STAT_L{i}_ORDERED_NAME'] = result[f'NAME_FIN_STAT_L{i}'].apply(
                lambda x: x[3:] if len(str(x)) > 3 else x
            )
        
        # Sort by CODE_FIN_STAT_FULL
        result = result.sort_values('CODE_FIN_STAT_FULL').reset_index(drop=True)
        
        return result
    
    def _get_level_value(self, full_path: str, level_idx: int, current_level: int) -> str:
        """
        Extracts value at specific level from the full path
        Replicates the CASE WHEN get(split(...)) logic
        """
        parts = str(full_path).split(' | ')
        
        if level_idx < len(parts):
            return parts[level_idx].replace('"', '')
        else:
            # If level doesn't exist, use the current level value
            if current_level < len(parts):
                return parts[current_level].replace('"', '')
            return ''
    
    def _identify_leafs(self, coa_df: pd.DataFrame) -> pd.DataFrame:
        """
        Identifies leaf nodes (accounts with no children)
        """
        result = coa_df.copy()
        
        # Find all codes that are parents
        parent_codes = set(result['CODE_PARENT_FIN_STAT'].unique())
        
        # Leaf nodes are those that are NOT in the parent codes list
        result['NFLAG_IS_LEAF'] = result['CODE_FIN_STAT'].apply(
            lambda x: 1 if x not in parent_codes else 0
        )
        
        # Reorder columns to match SQL output
        column_order = [
            'NUM_FIN_STAT_LEVEL',
            'NUM_FIN_STAT_ORDER',
            'CODE_FIN_STAT_RANK',
            'CODE_FIN_STAT',
            'NAME_FIN_STAT',
            'NAME_FIN_STAT_INDENT',
            'CODE_PARENT_FIN_STAT',
            'NAME_FIN_STAT_PARENT',
            'NAME_FIN_STAT_ENG',
            'TYPE_ACCOUNT',
            'TYPE_FIN_STATEMENT',
            'NFLAG_IS_LEAF',
            'CODE_FIN_STAT_FULL',
            'NAME_FIN_STAT_FULL',
        ] + [f'CODE_FIN_STAT_L{i}' for i in range(1, 11)] + \
          [f'NAME_FIN_STAT_L{i}' for i in range(1, 11)] + \
          [f'NAME_FIN_STAT_L{i}_ORDERED_NAME' for i in range(1, 11)]
        
        # Only include columns that exist
        column_order = [col for col in column_order if col in result.columns]
        result = result[column_order]
        
        # Sort by TYPE_FIN_STATEMENT and NUM_FIN_STAT_ORDER
        result = result.sort_values(['TYPE_FIN_STATEMENT', 'NUM_FIN_STAT_ORDER']).reset_index(drop=True)
        
        return result
    
    def create_business_subunit_coa(self, business_unit_code: str = 'KBC', business_subunits: pd.DataFrame = None) -> pd.DataFrame:
        """
        Cross-joins COA with business subunits for a specific business unit
        Replicates: DC_KBC_COA
        """
        if self.coa_output is None:
            raise ValueError("COA must be transformed first. Call transform_coa() first.")
        
        if business_subunits is None:
            business_subunits = self.business_subunits
            
        if business_subunits is None:
            raise ValueError("Business subunits must be loaded first. Call load_business_subunits() first.")
        
        # Filter business subunits for the specified business subunit (using PK)
        filtered_subunits = business_subunits[
            business_subunits['PK_BUSINESS_SUBUNIT'] == business_unit_code
        ][['PK_BUSINESS_SUBUNIT']]
        
        # Cross join
        result = self.coa_output.merge(filtered_subunits, how='cross')
        
        # Reorder columns to put PK_BUSINESS_SUBUNIT first
        cols = ['PK_BUSINESS_SUBUNIT'] + [col for col in self.coa_output.columns]
        result = result[cols]
        
        return result
    
    def create_mapping_to_central_coa(self, business_unit_code: str = 'KBC', business_subunits: pd.DataFrame = None) -> pd.DataFrame:
        """
        Creates mapping from business unit COA to central (FININ) COA
        Replicates: DC_KBC_2FININ_COA
        """
        if self.coa_input is None:
            raise ValueError("COA input must be provided.")
        
        if business_subunits is None:
            business_subunits = self.business_subunits
            
        if business_subunits is None:
            raise ValueError("Business subunits must be loaded first.")
        
        # Filter business subunits for the specified business subunit (using PK)
        filtered_subunits = business_subunits[
            business_subunits['PK_BUSINESS_SUBUNIT'] == business_unit_code
        ][['PK_BUSINESS_SUBUNIT']]
        
        # Prepare base data
        base = self.coa_input[[
            'CODE_FIN_STAT',
            'FININ_CODE_FIN_STAT',
            'NAME_FIN_STAT'
        ]].copy()
        
        # Cross join with business subunits
        result = base.merge(filtered_subunits, how='cross')
        
        # Rename and add columns
        result = result.rename(columns={
            'PK_BUSINESS_SUBUNIT': 'FK_BUSINESS_SUBUNIT',
            'CODE_FIN_STAT': 'SOURCE_CODE_FIN_STAT'
        })
        
        result['DATEID_VALID_FROM'] = '20000101'
        result['DATEID_VALID_TO'] = '30000101'
        result['DESC_FININ'] = result['NAME_FIN_STAT'].str[:1024]
        
        # Select final columns
        result = result[[
            'FK_BUSINESS_SUBUNIT',
            'SOURCE_CODE_FIN_STAT',
            'FININ_CODE_FIN_STAT',
            'DATEID_VALID_FROM',
            'DATEID_VALID_TO',
            'DESC_FININ'
        ]]
        
        return result
    
    def debug_count_check(self) -> pd.DataFrame:
        """
        Checks if number of rows at input equals number of outputs
        Returns rows that are in input but not in output
        """
        if self.coa_input is None or self.coa_output is None:
            raise ValueError("Both input and output COA must be available.")
        
        # Left join to find missing rows
        debug = self.coa_input.merge(
            self.coa_output[['CODE_FIN_STAT']],
            on='CODE_FIN_STAT',
            how='left',
            indicator=True
        )
        
        # Filter rows that are only in input
        missing = debug[debug['_merge'] == 'left_only'][[
            'NUM_FIN_STAT_ORDER',
            'CODE_FIN_STAT',
            'NAME_FIN_STAT',
            'CODE_PARENT_FIN_STAT',
            'TYPE_ACCOUNT',
            'TYPE_FIN_STATEMENT',
            'NAME_FIN_STAT_ENG'
        ]]
        
        return missing


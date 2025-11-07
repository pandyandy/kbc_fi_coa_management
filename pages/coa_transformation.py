"""
COA Transformation and Validation Page
Processes COA data through the transformation pipeline
"""

import streamlit as st
import pandas as pd
from utils.coa_transformer import COATransformer
from utils.coa_data_manager import COADataManager


def show_coa_transformation(data_manager: COADataManager):
    """Display COA transformation interface"""
    
    # Add custom CSS for blue buttons
    st.markdown("""
    <style>
    .stButton > button {
        background-color: #297cf7 !important;
        color: white !important;
        border: none !important;
        border-radius: 4px !important;
    }
    .stButton > button:hover {
        background-color: #1e5bb8 !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.title("COA Transformation & Enrichment")
    
    st.markdown("""
    This page processes the Chart of Accounts through the transformation pipeline to:
    - Build hierarchical structure
    - Create level-based flattening (L1-L10)
    - Identify leaf nodes
    - Generate business subunit mappings
    - Create central COA mappings
    """)
    
    # Initialize transformer
    transformer = COATransformer()
    
    # Load business subunits from Keboola (with caching)
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader("Step 1: Load Business Subunits from Keboola")
    with col2:
        if st.button("🔄 Refresh", help="Reload business subunits from Keboola"):
            if 'business_subunits' in st.session_state:
                del st.session_state['business_subunits']
            st.rerun()
    
    # Check if we already have business subunits in session state
    if 'business_subunits' not in st.session_state or st.session_state['business_subunits'] is None:
        with st.spinner("Loading business subunits from Keboola..."):
            try:
                business_subunits = transformer.load_business_subunits_from_keboola()
                if not business_subunits.empty:
                    st.session_state['business_subunits'] = business_subunits
                    st.success(f"✅ Loaded {len(business_subunits)} business subunits from Keboola")
                else:
                    st.error("❌ No business subunits loaded from Keboola")
                    return
            except Exception as e:
                st.error(f"❌ Error loading business subunits from Keboola: {e}")
                return
    else:
        business_subunits = st.session_state['business_subunits']
        st.success(f"✅ Using cached business subunits ({len(business_subunits)} records)")
    
    # Show business subunits data
    with st.expander("View Business Subunits"):
        st.dataframe(business_subunits, use_container_width=True)
    
    st.markdown("---")
    
    # Transform COA
    st.subheader("Step 2: Transform COA Data")
    
    if st.button("🔄 Run Transformation", type="primary", use_container_width=True):
        with st.spinner("Processing COA transformation..."):
            try:
                # Get current COA data
                coa_input = data_manager.get_flat_data()
                
                if coa_input is None or coa_input.empty:
                    st.error("❌ No COA data available")
                    return
                
                # Run transformation
                coa_output = transformer.transform_coa(coa_input)
                
                # Store in session state
                st.session_state['coa_transformed'] = coa_output
                st.session_state['transformer'] = transformer
                
                st.success(f"✅ Transformation complete! Processed {len(coa_output)} accounts")
                
                # Show summary
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    max_level = coa_output['NUM_FIN_STAT_LEVEL'].max()
                    st.metric("Max Hierarchy Level", max_level)
                with col2:
                    leaf_count = coa_output['NFLAG_IS_LEAF'].sum()
                    st.metric("Leaf Nodes", leaf_count)
                with col3:
                    bs_count = len(coa_output[coa_output['TYPE_FIN_STATEMENT'] == 'BS'])
                    st.metric("Balance Sheet", bs_count)
                with col4:
                    pl_count = len(coa_output[coa_output['TYPE_FIN_STATEMENT'] == 'PL'])
                    st.metric("Profit & Loss", pl_count)
                
            except Exception as e:
                st.error(f"❌ Transformation error: {e}")
                st.exception(e)
    
    st.markdown("---")
    
    # Show results if available
    if 'coa_transformed' in st.session_state and st.session_state['coa_transformed'] is not None:
        st.subheader("Step 3: View Transformation Results")
        
        tab1, tab2, tab3, tab4 = st.tabs([
            "Enriched COA",
            "Business Subunit COA", 
            "Central COA Mapping",
            "Debug Info"
        ])
        
        with tab1:
            st.markdown("**Enriched Chart of Accounts with hierarchy levels**")
            coa_output = st.session_state['coa_transformed']
            
            # Display options
            show_all_cols = st.checkbox("Show all columns (including L1-L10)", value=False)
            
            if show_all_cols:
                st.dataframe(coa_output, use_container_width=True, height=500)
            else:
                # Show key columns only
                key_cols = [
                    'NUM_FIN_STAT_LEVEL',
                    'NUM_FIN_STAT_ORDER',
                    'CODE_FIN_STAT',
                    'NAME_FIN_STAT',
                    'NAME_FIN_STAT_INDENT',
                    'CODE_PARENT_FIN_STAT',
                    'TYPE_ACCOUNT',
                    'TYPE_FIN_STATEMENT',
                    'NFLAG_IS_LEAF'
                ]
                st.dataframe(coa_output[key_cols], use_container_width=True, height=500)
            
            # Download button
            csv = coa_output.to_csv(index=False)
            st.download_button(
                label="📥 Download Enriched COA",
                data=csv,
                file_name="dc_coa_enriched.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with tab2:
            st.markdown("**COA cross-joined with Business Subunits**")
            
            transformer = st.session_state.get('transformer')
            business_subunits = st.session_state.get('business_subunits')
            if transformer and business_subunits is not None:
                # Select business subunit
                business_units = business_subunits['PK_BUSINESS_SUBUNIT'].unique().tolist()
                selected_bu = st.selectbox("Select Business Subunit", business_units)
                
                if st.button("Generate Business Subunit COA", use_container_width=True):
                    try:
                        bu_coa = transformer.create_business_subunit_coa(selected_bu, business_subunits)
                        st.success(f"✅ Generated {len(bu_coa)} records for {selected_bu}")
                        st.dataframe(bu_coa.head(100), use_container_width=True, height=400)
                        
                        # Download button
                        csv = bu_coa.to_csv(index=False)
                        st.download_button(
                            label=f"📥 Download {selected_bu} COA",
                            data=csv,
                            file_name=f"dc_{selected_bu.lower()}_coa.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
            else:
                st.error("❌ Business subunits must be loaded first. Please load them in Step 1.")
        
        with tab3:
            st.markdown("**Mapping to Central (FININ) COA**")
            
            transformer = st.session_state.get('transformer')
            business_subunits = st.session_state.get('business_subunits')
            if transformer and business_subunits is not None:
                # Select business subunit
                business_units = business_subunits['PK_BUSINESS_SUBUNIT'].unique().tolist()
                selected_bu = st.selectbox("Select Business Subunit for Mapping", business_units, key="mapping_bu")
                
                if st.button("Generate Central COA Mapping", use_container_width=True):
                    try:
                        mapping = transformer.create_mapping_to_central_coa(selected_bu, business_subunits)
                        st.success(f"✅ Generated {len(mapping)} mapping records")
                        st.dataframe(mapping, use_container_width=True, height=400)
                        
                        # Download button
                        csv = mapping.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Mapping",
                            data=csv,
                            file_name=f"dc_{selected_bu.lower()}_2finin_coa.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    except Exception as e:
                        st.error(f"❌ Error: {e}")
            else:
                st.error("❌ Business subunits must be loaded first. Please load them in Step 1.")
        
        with tab4:
            st.markdown("**Debug: Missing Records Check**")
            st.info("Shows records that are in the input but missing from the transformed output")
            
            transformer = st.session_state.get('transformer')
            if transformer:
                try:
                    debug_results = transformer.debug_count_check()
                    
                    if debug_results.empty:
                        st.success("✅ All input records are present in the output!")
                    else:
                        st.warning(f"⚠️ Found {len(debug_results)} missing records:")
                        st.dataframe(debug_results, use_container_width=True)
                except Exception as e:
                    st.error(f"❌ Error: {e}")


"""
Point d'entrée du dashboard : déclare les pages, leurs titres et leurs URL.

    streamlit run dashboard/app.py

Navigation explicite (st.navigation) plutôt que le dossier pages/ : le nom affiché
de chaque page ne dépend plus du nom de son fichier.
"""

import streamlit as st

st.set_page_config(page_title="Finance Data Platform", page_icon="📈", layout="wide")

navigation = st.navigation([
    st.Page("quotidien.py", title="Quotidien", icon="📈", default=True),
    st.Page("live.py", title="Live", icon="⚡", url_path="Live"),
])
navigation.run()

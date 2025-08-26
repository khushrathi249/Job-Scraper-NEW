# setup.py
import streamlit as st
import subprocess
import sys

@st.cache_resource
def setup_playwright():
    """
    Downloads the necessary Playwright browsers without system dependencies.
    """
    st.info("Setting up Playwright browsers...")
    try:
        # Command without --with-deps to avoid sudo
        command = [sys.executable, "-m", "playwright", "install"]
        
        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True
        )
        st.success("Playwright setup successful.")
        print(process.stdout)
        return True
    except subprocess.CalledProcessError as e:
        st.error(f"Failed to install Playwright browsers: {e.stderr}")
        print(e.stderr)
        return False
    except Exception as e:
        st.error(f"An unexpected error occurred during Playwright setup: {e}")
        return False

# setup.py
import streamlit as st
import subprocess
import sys

# Use Streamlit's caching to run this setup only once.
@st.cache_resource
def setup_playwright():
    """
    Downloads the necessary Playwright browsers.
    """
    print("Setting up Playwright...")
    try:
        # The command to run. We use sys.executable to ensure we're using the same python env.
        command = [sys.executable, "-m", "playwright", "install", "--with-deps"]
        # --with-deps will also install system dependencies, making packages.txt less critical
        
        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,  # This will raise an exception if the command fails
            text=True
        )
        print("Playwright setup successful.")
        print(process.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print("Playwright setup failed.")
        print(e.stderr)
        # Display the error in the Streamlit app
        st.error(f"Failed to install Playwright browsers: {e.stderr}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during setup: {e}")
        st.error(f"An unexpected error occurred during Playwright setup: {e}")
        return False
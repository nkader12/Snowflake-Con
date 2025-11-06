"""
Utility functions for the Snowflake connector package.
"""

import sys
import getpass
from typing import Optional


def is_jupyter() -> bool:
    """
    Detect if we're running in a Jupyter notebook environment.
    
    Returns:
        True if running in Jupyter, False otherwise
    """
    try:
        # Check for IPython/Jupyter
        if 'ipykernel' in sys.modules:
            return True
        # Check for IPython
        if 'IPython' in sys.modules:
            from IPython import get_ipython
            ipython = get_ipython()
            if ipython is not None:
                return ipython.__class__.__name__ == 'ZMQInteractiveShell'
    except (ImportError, AttributeError):
        pass
    return False


def get_password(prompt: str = "Enter Snowflake password: ") -> str:
    """
    Get password input that works in both Jupyter notebooks and regular Python.
    
    Args:
        prompt: The prompt message to display
        
    Returns:
        The password string entered by the user
    """
    try:
        # Try getpass first (works in most environments)
        password = getpass.getpass(prompt)
        return password
    except (EOFError, KeyboardInterrupt):
        # Handle cases where getpass might fail
        raise
    except Exception:
        # Fallback for environments where getpass doesn't work
        # This is less secure but ensures functionality
        if is_jupyter():
            # In Jupyter, try using IPython's getpass
            try:
                from IPython.core.getpass import getpass as ipython_getpass
                return ipython_getpass(prompt)
            except ImportError:
                # Last resort: use input with a warning
                import warnings
                warnings.warn(
                    "Password input will be visible. Consider using getpass in a terminal.",
                    UserWarning
                )
                return input(prompt).strip()
        else:
            # In regular Python, getpass should work, but if it doesn't, raise
            raise


def get_input(prompt: str) -> str:
    """
    Get user input that works in both Jupyter notebooks and regular Python.
    
    Args:
        prompt: The prompt message to display
        
    Returns:
        The input string entered by the user
    """
    return input(prompt).strip()


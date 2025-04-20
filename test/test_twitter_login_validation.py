#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script for Twitter login validation.

This script confirms whether our login approach successfully authenticates
with Twitter and shows the logged-in view of profiles. It operates in
non-headless mode so the login process can be observed.
"""

import os
import sys
import time
import json
import logging
from datetime import datetime
from pathlib import Path

# Add parent directory to path to import from etl module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from utils.logging_config import setup_logging, logger

def validate_twitter_login():
    """
    Test Twitter login and validate the login state.
    
    Runs a browser in visible mode to show the login flow,
    takes screenshots, and verifies that we see the logged-in
    state on Twitter profiles.
    """
    # Twitter credentials
    username = os.environ.get("TWITTER_USERNAME", "")
    password = os.environ.get("TWITTER_PASSWORD", "")
    
    # Check if credentials are provided
    if not username or not password:
        logger.error("Twitter credentials not provided. Set TWITTER_USERNAME and TWITTER_PASSWORD environment variables.")
        return False
    
    # Create directories
    output_dir = Path("data/twitter_validation")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    screenshots_dir = Path("data/twitter_validation/screenshots")
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    
    storage_state_path = output_dir / "twitter_validation_storage_state.json"
    
    # Test a recent financial tweets handle
    test_handle = "matt_willemsen"
    
    login_success = False
    
    with sync_playwright() as p:
        # Launch browser in non-headless mode to observe the process
        browser = p.chromium.launch(
            headless=False,
            slow_mo=100  # Slow down actions for visibility
        )
        
        # Check if storage state exists
        use_storage = False
        if storage_state_path.exists():
            logger.info(f"Found existing storage state at {storage_state_path}")
            try:
                # First create context with the storage state
                context = browser.new_context(
                    storage_state=str(storage_state_path),
                    viewport={"width": 1280, "height": 800}
                )
                logger.info("Created browser context with existing storage state")
                use_storage = True
            except Exception as e:
                logger.warning(f"Failed to use existing storage state: {e}")
                use_storage = False
        
        # If no storage or failed to use it, create fresh context
        if not use_storage:
            context = browser.new_context(
                viewport={"width": 1280, "height": 800}
            )
            logger.info("Created fresh browser context")
        
        # Create page
        page = context.new_page()
        
        # Take initial screenshot
        page.screenshot(path=str(screenshots_dir / "start.png"))
        logger.info("Took initial screenshot")
        
        # First try going directly to Twitter and see if we're logged in
        if use_storage:
            logger.info("Attempting to access Twitter with stored cookies")
            page.goto("https://twitter.com/home", wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)  # Give some time for the page to stabilize
            
            page.screenshot(path=str(screenshots_dir / "home_with_cookies.png"))
            logger.info("Took screenshot of home page with cookies")
            
            # Check login state by looking for login buttons
            login_buttons = page.locator("a:has-text('Log in'), a:has-text('Sign up'), div:has-text('Sign in')").count()
            
            if login_buttons == 0:
                logger.info("Appears to be logged in with stored cookies!")
                login_success = True
            else:
                logger.warning("Not logged in with stored cookies, will try fresh login")
        
        # If not already logged in, perform manual login
        if not login_success:
            logger.info("Starting manual login process")
            
            # Go to login page
            page.goto("https://twitter.com/i/flow/login", wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)  # Wait for login form to fully load
            
            page.screenshot(path=str(screenshots_dir / "login_page.png"))
            logger.info("Loaded login page")
            
            # Enter email
            try:
                page.wait_for_selector("input[autocomplete='username']", state="visible", timeout=10000)
                page.fill("input[autocomplete='username']", username)
                logger.info("Entered username/email")
                page.screenshot(path=str(screenshots_dir / "entered_email.png"))
                
                # Find the Next button using JavaScript
                logger.info("Clicking Next button using JavaScript")
                try:
                    page.evaluate("""
                        var buttons = Array.from(document.querySelectorAll('div[role="button"]'));
                        var nextButton = buttons.find(b => b.textContent.includes('Next'));
                        if (nextButton) nextButton.click();
                    """)
                    logger.info("Clicked Next button via JavaScript")
                except Exception as e:
                    logger.error(f"Failed to click Next button via JavaScript: {e}")
                    
                # Wait longer for the password screen to appear    
                time.sleep(5)  # Increased wait time to ensure the next screen loads
                page.wait_for_load_state("networkidle", timeout=10000)
                page.screenshot(path=str(screenshots_dir / "after_next_button.png"))
                
                # Check if Twitter asks for username specifically
                try:
                    username_field = page.locator("input[data-testid='ocfEnterTextTextInput']")
                    if username_field.is_visible(timeout=3000):
                        logger.info("Twitter is asking for username specifically")
                        username_handle = username.split('@')[0] if '@' in username else username
                        page.fill("input[data-testid='ocfEnterTextTextInput']", username_handle)
                        logger.info(f"Entered username: {username_handle}")
                        page.screenshot(path=str(screenshots_dir / "entered_username.png"))
                        
                        # Click Next again using JavaScript
                        logger.info("Clicking Next button after username using JavaScript")
                        try:
                            page.evaluate("""
                                var buttons = Array.from(document.querySelectorAll('div[role="button"]'));
                                var nextButton = buttons.find(b => b.textContent.includes('Next'));
                                if (nextButton) nextButton.click();
                            """)
                            logger.info("Clicked Next button after username via JavaScript")
                        except Exception as e:
                            logger.error(f"Failed to click Next button after username via JavaScript: {e}")
                            
                        time.sleep(5)  # Increased wait time for password field
                        page.wait_for_load_state("networkidle", timeout=10000)
                except Exception as e:
                    logger.info(f"No username field requested: {e}")
                
                # Enter password - try multiple approaches
                try:
                    # Take screenshot to debug
                    page.screenshot(path=str(screenshots_dir / "before_password.png"))
                    logger.info("Checking for password field")
                    
                    # Try to locate password field using different methods
                    password_selector_found = False
                    password_selectors = [
                        "input[name='password']", 
                        "input[autocomplete='current-password']", 
                        "input[type='password']"
                    ]
                    
                    for selector in password_selectors:
                        try:
                            logger.info(f"Trying password selector: {selector}")
                            if page.is_visible(selector, timeout=3000):
                                page.fill(selector, password)
                                password_selector_found = True
                                logger.info(f"Entered password using selector: {selector}")
                                break
                        except Exception as e:
                            logger.debug(f"Password selector {selector} failed: {e}")
                    
                    # If standard selectors didn't work, try JavaScript approach
                    if not password_selector_found:
                        logger.info("Trying to find password field via JavaScript")
                        try:
                            # Count all input fields
                            input_count = page.evaluate("""
                                document.querySelectorAll('input').length
                            """)
                            logger.info(f"Found {input_count} input fields on page")
                            
                            # Try to find password field by evaluating all inputs
                            found_pwd = page.evaluate("""
                                var inputs = Array.from(document.querySelectorAll('input'));
                                var pwdInput = inputs.find(i => 
                                    i.type === 'password' || 
                                    i.name === 'password' || 
                                    i.autocomplete === 'current-password');
                                
                                if (pwdInput) {
                                    pwdInput.value = arguments[0];
                                    return true;
                                }
                                return false;
                            """, password)
                            
                            if found_pwd:
                                logger.info("Found and filled password field via JavaScript")
                                password_selector_found = True
                            else:
                                logger.error("Could not find password field via JavaScript")
                        except Exception as e:
                            logger.error(f"JavaScript password approach failed: {e}")
                    
                    if not password_selector_found:
                        logger.error("Could not find password field through any method")
                        page.screenshot(path=str(screenshots_dir / "password_not_found.png"))
                        return False
                        
                    page.screenshot(path=str(screenshots_dir / "entered_password.png"))
                    
                    # Click Login button using JavaScript
                    logger.info("Clicking Login button using JavaScript")
                    try:
                        page.evaluate("""
                            var buttons = Array.from(document.querySelectorAll('div[role="button"]'));
                            var loginButton = buttons.find(b => 
                                b.textContent.includes('Log in') || 
                                b.textContent.includes('Sign in') || 
                                b.textContent.includes('Login'));
                            if (loginButton) loginButton.click();
                        """)
                        logger.info("Clicked Login button via JavaScript")
                    except Exception as e:
                        logger.error(f"Failed to click Login button via JavaScript: {e}")
                    
                    page.wait_for_load_state("networkidle", timeout=30000)
                    page.screenshot(path=str(screenshots_dir / "after_login.png"))
                    
                    # Wait a bit for the login to complete
                    time.sleep(5)
                    
                    # Check login state by looking for login buttons
                    login_buttons = page.locator("a:has-text('Log in'), a:has-text('Sign up'), div:has-text('Sign in')").count()
                    
                    if login_buttons == 0:
                        logger.info("Successfully logged in!")
                        login_success = True
                        
                        # Save storage state for future use
                        context.storage_state(path=str(storage_state_path))
                        logger.info(f"Saved storage state to {storage_state_path}")
                    else:
                        logger.error("Login failed, still seeing login buttons")
                        page.screenshot(path=str(screenshots_dir / "login_failed.png"))
                        login_success = False
                except Exception as e:
                    logger.error(f"Error during password entry or login: {e}")
                    page.screenshot(path=str(screenshots_dir / "login_error.png"))
                    login_success = False
            except Exception as e:
                logger.error(f"Error during email entry: {e}")
                page.screenshot(path=str(screenshots_dir / "email_error.png"))
                login_success = False
        
        # Now let's check a profile to validate we see the logged-in view
        if login_success:
            logger.info(f"Testing profile view for @{test_handle}")
            try:
                # Go to the profile
                page.goto(f"https://twitter.com/{test_handle}", wait_until="domcontentloaded", timeout=30000)
                time.sleep(3)  # Wait a bit for content to load
                
                # Take screenshot of the profile
                page.screenshot(path=str(screenshots_dir / f"{test_handle}_profile.png"))
                logger.info(f"Took screenshot of @{test_handle}'s profile")
                
                # Try to find tweets
                tweet_elements = page.locator("[data-testid='tweet']").count()
                logger.info(f"Found {tweet_elements} tweet elements on profile")
                
                # Check if we can see the Post/Reply/Media tabs (only visible when logged in)
                tabs = page.locator("a[role='tab']:has-text('Posts'), a[role='tab']:has-text('Replies'), a[role='tab']:has-text('Media')").count()
                
                if tabs > 0:
                    logger.info("Found Posts/Replies/Media tabs - confirms we're logged in!")
                    
                    # Try to get timestamp of first tweet
                    try:
                        tweet = page.locator("[data-testid='tweet']").first
                        time_element = tweet.locator("time").first
                        if time_element.is_visible():
                            datetime_attr = time_element.get_attribute("datetime")
                            logger.info(f"First tweet timestamp: {datetime_attr}")
                    except Exception as e:
                        logger.warning(f"Couldn't extract tweet timestamp: {e}")
                else:
                    logger.warning("Couldn't find Posts/Replies/Media tabs - might not be properly logged in")
                
            except Exception as e:
                logger.error(f"Error checking profile: {e}")
                page.screenshot(path=str(screenshots_dir / "profile_error.png"))
        
        # Close browser
        browser.close()
    
    # Final result
    if login_success:
        logger.info("✓ Login validation SUCCESSFUL")
        logger.info(f"Screenshots saved to {screenshots_dir}")
        return True
    else:
        logger.error("✗ Login validation FAILED")
        logger.info(f"Check screenshots in {screenshots_dir} for troubleshooting")
        return False

if __name__ == "__main__":
    # Set up logging
    setup_logging()
    
    # Run the validation
    logger.info("Starting Twitter login validation test")
    result = validate_twitter_login()
    
    # Exit with appropriate code
    sys.exit(0 if result else 1) 
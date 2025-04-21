"""
Twitter extraction using Playwright and OpenAI Vision.

This module provides a Twitter extractor that uses OpenAI's vision capabilities
to guide Playwright browser automation for Twitter data extraction.
"""

import os
import json
import base64
import random
import logging
import asyncio
import tempfile
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path

import openai
from playwright.async_api import async_playwright, Page, BrowserContext, Browser

from utils.logging_config import logger

class VisionTwitterExtractor:
    """
    Extract tweets from Twitter using OpenAI Vision to guide browser automation.
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """Initialize the Twitter extractor."""
        self.params = params or {}
        
        # Extract parameters with defaults
        self.username = self.params.get('username', '')
        self.password = self.params.get('password', '')
        self.verification_code = self.params.get('verification_code', '')
        self.email = self.params.get('email', '')
        self.handles = self.params.get('handles', [])
        self.max_tweets_per_handle = self.params.get('max_tweets_per_handle', 10)
        self.output_file = self.params.get('output_file', 'twitter_extraction_results.json')
        self.screenshots_dir = self.params.get('screenshots_dir', 'screenshots')
        self.headless = self.params.get('headless', True)
        self.user_data_dir = self.params.get('user_data_dir', os.path.join(tempfile.gettempdir(), 'twitter_browser_data'))
        
        # OpenAI configuration
        self.openai_api_key = self.params.get('openai_api_key') or os.environ.get('OPENAI_API_KEY', '')
        self.model = self.params.get('model', 'gpt-4o')
        
        # OpenAI client
        openai.api_key = self.openai_api_key
        self.client = openai.OpenAI(api_key=self.openai_api_key)
        
        # Create directories
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        os.makedirs(self.screenshots_dir, exist_ok=True)
        
        # Playwright resources
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        
        # Initialize logger
        self.logger = logger
    
    async def extract(self) -> Dict[str, Any]:
        """Extract tweets from Twitter."""
        try:
            self.logger.info(f"Starting Twitter extraction for handles: {self.handles}")
            
            # Initialize browser
            await self._setup_browser()
            
            # Log in to Twitter
            logged_in = await self._login()
            
            if not logged_in:
                self.logger.error("Failed to login to Twitter")
                return {"tweets_by_handle": {}, "error": "Login failed"}
            
            # Extract tweets for each handle
            tweets_by_handle = {}
            
            for handle in self.handles:
                self.logger.info(f"Extracting tweets for @{handle}")
                
                # Navigate to user's profile
                profile_url = f"https://twitter.com/{handle}"
                try:
                    await self.page.goto(profile_url, wait_until="domcontentloaded")
                    await asyncio.sleep(self._random_delay(2, 4))
                    
                    # Extract tweets
                    handle_tweets = await self._extract_tweets_for_handle(handle)
                    
                    tweets_by_handle[handle] = handle_tweets
                    self.logger.info(f"Extracted {len(handle_tweets)} tweets from @{handle}")
                    
                except Exception as e:
                    self.logger.error(f"Error extracting tweets for @{handle}: {e}")
                    tweets_by_handle[handle] = []
            
            # Close browser
            await self._close_browser()
            
            # Prepare result
            result = {
                "tweets_by_handle": tweets_by_handle,
                "metadata": {
                    "handles": self.handles,
                    "total_tweets": sum(len(tweets) for tweets in tweets_by_handle.values()),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            }
            
            # Save to output file
            self._save_output(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in extraction: {e}")
            await self._close_browser()
            return {
                "tweets_by_handle": {},
                "error": str(e),
                "metadata": {
                    "handles": self.handles,
                    "total_tweets": 0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            }
    
    async def _setup_browser(self) -> None:
        """Initialize the browser and set up a new page."""
        self.playwright = await async_playwright().start()
        
        # Enhanced browser arguments based on ZenRows recommendations
        browser_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins",
            "--disable-site-isolation-trials",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--disable-gpu",
            "--hide-scrollbars",
            "--no-first-run",
            "--no-default-browser-check",
            "--no-zygote",
            "--window-size=1920,1080",
            # Additional args from ZenRows
            "--font-render-hinting=none",
            "--disable-features=IsolateOrigins,site-per-process,SitePerProcess",
            "--disable-web-security",
            "--ignore-certificate-errors",
            "--ignore-certificate-errors-spki-list",
            f"--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ]

        # Enhanced context parameters
        context_params = {
            "viewport": {"width": 1920, "height": 1080},
            "screen": {"width": 1920, "height": 1080},  # Added screen size
            "color_scheme": "dark",
            "locale": "en-AU",
            "timezone_id": "Australia/Perth",
            "geolocation": {
                "latitude": -31.9523,
                "longitude": 115.8613,
                "accuracy": 100,
            },
            "permissions": ["geolocation", "notifications"],
            "ignore_https_errors": True,
            "has_touch": False,
            "is_mobile": False,
            "device_scale_factor": 2,
            "accept_downloads": False,  # Added from ZenRows
            "java_script_enabled": True,  # Added from ZenRows
            "bypass_csp": True,  # Added from ZenRows
        }

        try:
            user_data_path = os.path.abspath(self.user_data_dir)
            self.logger.info(f"Using browser profile directory: {user_data_path}")

            # Enhanced stealth scripts from ZenRows
            await self._inject_enhanced_stealth_scripts()
            
            # Launch persistent context
            self.browser = await self.playwright.chromium.launch_persistent_context(
                user_data_dir=user_data_path,
                headless=self.headless,
                args=browser_args,
                **context_params
            )
            
            if len(self.browser.pages) > 0:
                self.page = self.browser.pages[0]
            else:
                self.page = await self.browser.new_page()

            self.logger.info("Browser setup complete with enhanced stealth configuration")
        except Exception as e:
            self.logger.error(f"Error setting up browser: {e}")
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            raise
    
    async def _close_browser(self) -> None:
        """Close browser and Playwright instance."""
        try:
            if self.page:
                await self.page.close()
                self.page = None
            
            if self.context:
                await self.context.close()
                self.context = None
            
            if self.browser:
                await self.browser.close()
                self.browser = None
            
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            
            self.logger.info("Browser closed")
        except Exception as e:
            self.logger.error(f"Error closing browser: {e}")
    
    async def _login(self) -> bool:
        """Log in to Twitter using OpenAI vision guidance."""
        try:
            self.logger.info("Starting Twitter login with OpenAI vision guidance")
            
            # Navigate to Twitter login page
            await self.page.goto("https://twitter.com/i/flow/login", wait_until="domcontentloaded")
            await asyncio.sleep(self._random_delay(1, 2))
            
            # Take screenshot of initial login page
            screenshot_path = await self._take_screenshot("initial_login_page")
            
            # Loop until logged in or max steps reached
            max_steps = 15
            for step in range(1, max_steps + 1):
                self.logger.info(f"Login step {step}/{max_steps}")
                
                # Check if already logged in
                if await self._check_if_logged_in():
                    self.logger.info("Successfully logged into Twitter")
                    return True
                
                # Take screenshot of current page
                screenshot_path = await self._take_screenshot(f"login_step_{step}")
                
                # Get page text for context
                page_text = await self._get_page_text()
                
                # Prepare context for OpenAI
                context = self._prepare_login_context(
                    step=step,
                    screenshot_path=screenshot_path,
                    page_content=page_text,
                    username=self.username,
                    verification_code=self.verification_code
                )
                
                # Get guidance from OpenAI
                guidance = await self._get_openai_guidance(screenshot_path, context)
                
                # Process the response
                success = await self._process_guidance(guidance, step)
                
                # Break if an action fails
                if not success:
                    self.logger.warning(f"Action failed in step {step}")
                    break
                
                # Add delay between steps
                await asyncio.sleep(self._random_delay(1, 2))
            
            # Final check after loop
            is_logged_in = await self._check_if_logged_in()
            if is_logged_in:
                self.logger.info("Successfully logged into Twitter")
                return True
            
            self.logger.error("Failed to login to Twitter after maximum attempts")
            return False
            
        except Exception as e:
            self.logger.error(f"Error during Twitter login: {e}")
            return False
    
    async def _extract_tweets_for_handle(self, handle: str) -> List[Dict[str, Any]]:
        """Extract tweets for a specific Twitter handle."""
        try:
            self.logger.info(f"Starting tweet extraction for @{handle}")
            tweets = []
            max_scroll_attempts = 10
            tweets_seen = set()  # To track unique tweet IDs
            
            # Take a screenshot of the profile page
            screenshot_path = await self._take_screenshot(f"profile_{handle}")
            
            # Prepare extraction context for OpenAI
            context = self._prepare_extraction_context(handle=handle)
            
            # Loop until we have enough tweets or reach max scrolls
            for scroll_attempt in range(1, max_scroll_attempts + 1):
                self.logger.info(f"Extraction attempt {scroll_attempt}/{max_scroll_attempts}")
                
                # Get tweets from current view
                current_tweets = await self._extract_tweets_from_current_view(handle)
                
                # Add new unique tweets
                for tweet in current_tweets:
                    tweet_id = tweet.get('id', '')
                    if tweet_id and tweet_id not in tweets_seen:
                        tweets_seen.add(tweet_id)
                        tweets.append(tweet)
                        self.logger.info(f"Found new tweet: {tweet.get('content', '')[:50]}...")
                
                # Check if we have enough tweets
                if len(tweets) >= self.max_tweets_per_handle:
                    self.logger.info(f"Reached maximum number of tweets ({self.max_tweets_per_handle}) for @{handle}")
                    break
                
                # Take screenshot for OpenAI
                screenshot_path = await self._take_screenshot(f"extraction_{handle}_{scroll_attempt}")
                
                # Get OpenAI guidance
                guidance = await self._get_openai_guidance(screenshot_path, context)
                
                # Process the response
                success = await self._process_guidance(guidance, scroll_attempt)
                if not success:
                    self.logger.warning(f"Action failed in extraction attempt {scroll_attempt}")
                    break
                
                # Add delay between scrolls
                await asyncio.sleep(self._random_delay(1, 2))
            
            # Return tweets up to the maximum
            return tweets[:self.max_tweets_per_handle]
        
        except Exception as e:
            self.logger.error(f"Error extracting tweets for @{handle}: {e}")
            return []
    
    async def _extract_tweets_from_current_view(self, handle: str) -> List[Dict[str, Any]]:
        """Extract tweets from the current view of the page."""
        try:
            # Use JavaScript to extract tweets
            tweets_data = await self.page.evaluate("""(handle) => {
                const tweets = [];
                
                // Find all article elements (tweets)
                const articles = Array.from(document.querySelectorAll('article[data-testid="tweet"]'));
                
                for (const article of articles) {
                    try {
                        // Get tweet ID from the element
                        const tweetLinkElement = article.querySelector('a[href*="/status/"]');
                        if (!tweetLinkElement) continue;
                        
                        const href = tweetLinkElement.getAttribute('href');
                        const idMatch = href.match(/\\/status\\/(\\d+)/);
                        const id = idMatch ? idMatch[1] : '';
                        
                        // Skip if no valid ID
                        if (!id) continue;
                        
                        // Use full article innerText to capture retweets/quotes
                        let content = article.innerText ? article.innerText.trim() : '';

                        // Extract all image URLs in the tweet (including quoted tweet images)
                        const imageEls = Array.from(article.querySelectorAll('img'));
                        const image_urls = imageEls.map(img => img.src).filter(src => src);

                        // Extract external links in the tweet text
                        const linkEls = Array.from(article.querySelectorAll('a[href]'));
                        const external_links = linkEls.map(a => a.href).filter(href => href && href.startsWith('http'));
                        
                        // Get timestamp
                        const timeElement = article.querySelector('time');
                        const datetime = timeElement ? timeElement.getAttribute('datetime') : '';
                        
                        // Get engagement metrics
                        const metricsElements = article.querySelectorAll('[data-testid="reply"], [data-testid="retweet"], [data-testid="like"]');
                        const metrics = {};
                        
                        for (const element of metricsElements) {
                            const testId = element.getAttribute('data-testid');
                            const valueElement = element.querySelector('span[data-testid="app-text-transition-container"]');
                            const value = valueElement ? valueElement.textContent.trim() : '0';
                            
                            if (testId === 'reply') metrics.replies = value;
                            else if (testId === 'retweet') metrics.retweets = value;
                            else if (testId === 'like') metrics.likes = value;
                        }
                        
                        // Get author info
                        const authorElement = article.querySelector('[data-testid="User-Name"]');
                        const author = {
                            handle: handle,
                            display_name: authorElement ? authorElement.textContent.split('@')[0].trim() : ''
                        };
                        
                        // Create tweet object
                        const tweet = {
                            id: id,
                            content: content,
                            timestamp: datetime,
                            author: author,
                            username: handle,
                            image_urls: image_urls,
                            external_links: external_links,
                            metrics: metrics,
                            url: `https://twitter.com/${handle}/status/${id}`
                        };
                        
                        tweets.push(tweet);
                    } catch (error) {
                        console.error('Error extracting tweet:', error);
                    }
                }
                
                return tweets;
            }""", handle)
            
            return tweets_data
        
        except Exception as e:
            self.logger.error(f"Error extracting tweets from current view: {e}")
            return []
    
    def _prepare_extraction_context(self, handle: str) -> str:
        """Prepare context for OpenAI to assist with tweet extraction."""
        return f"""I am trying to extract tweets from Twitter/X for the user @{handle}. Please analyze the screenshot and tell me what to do next.

I need to:
1. Scroll down to see more tweets
2. Make sure I'm seeing the main timeline
3. Extract tweet content, timestamp, and metrics

I need you to:
1. Analyze what's happening on the screen
2. Tell me the next action to take
3. Provide necessary parameters (selector, direction, etc.)

Return a JSON response with the following structure:
{{
  "analysis": "Brief description of what you see on screen and what needs to be done",
  "action": "scroll/click_element/wait",
  "selector": "CSS selector for the element (if applicable)",
  "button_text": "Text on button to click (if applicable)",
  "wait_time": 2000,
  "direction": "up/down",
  "distance": 800
}}
"""
    
    async def _get_openai_guidance(self, screenshot_path: str, context: str) -> Dict[str, Any]:
        """Get guidance from OpenAI Vision API."""
        try:
            # Ensure the image is of reasonable size
            from PIL import Image
            try:
                with Image.open(screenshot_path) as img:
                    # Resize if needed (max 2000x2000)
                    width, height = img.size
                    if width > 2000 or height > 2000:
                        new_width = min(width, 2000)
                        new_height = min(height, 2000)
                        img = img.resize((new_width, new_height))
                        # Save resized image to a temp file
                        temp_path = f"{screenshot_path}_resized.jpg"
                        img.save(temp_path, format="JPEG", quality=85)
                        screenshot_path = temp_path
                        self.logger.info(f"Resized image to {new_width}x{new_height} for API compatibility")
            except Exception as e:
                self.logger.warning(f"Error processing image (continuing with original): {e}")
            
            # Encode the screenshot
            with open(screenshot_path, "rb") as image_file:
                base64_image = base64.b64encode(image_file.read()).decode('utf-8')
            
            # Create the OpenAI Chat request
            self.logger.info(f"Sending request to OpenAI API with model: {self.model}")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": context},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=1500,
                temperature=0.2
            )
            
            # Get the response content
            message_content = response.choices[0].message.content
            
            # Log the full raw response
            self.logger.info(f"Raw OpenAI response: {message_content}")
            
            # Check if OpenAI was unable to view the image
            cant_see_image = False
            if "unable to view" in message_content.lower() or "can't view" in message_content.lower() or "cannot view" in message_content.lower():
                self.logger.warning("OpenAI indicated it cannot view the image")
                cant_see_image = True
            
            # Extract JSON from the response
            import re
            json_match = re.search(r'```json\s*(.*?)\s*```|({.*})', message_content, re.DOTALL)
            
            if json_match:
                json_content = json_match.group(1) or json_match.group(2)
                try:
                    # Remove any comments from the JSON
                    json_content = re.sub(r'//.*?$', '', json_content, flags=re.MULTILINE)
                    json_content = re.sub(r'/\*.*?\*/', '', json_content, flags=re.DOTALL)
                    
                    parsed_guidance = json.loads(json_content)
                    
                    # Add the cant_see_image flag
                    parsed_guidance["can_see_image"] = not cant_see_image
                    
                    self.logger.info(f"OpenAI analysis: {parsed_guidance.get('analysis', '')}")
                    return parsed_guidance
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Could not parse JSON from OpenAI response: {e}")
            
            # If we couldn't extract JSON, create a basic structure
            self.logger.warning("Could not extract JSON from OpenAI response, using basic structure")
            return {
                "action": "wait" if cant_see_image else "scroll",
                "analysis": message_content,
                "wait_time": 2000,
                "can_see_image": not cant_see_image
            }
            
        except Exception as e:
            self.logger.error(f"Error getting OpenAI guidance: {e}")
            # Return basic guidance as fallback
            return {"action": "wait", "wait_time": 2000, "can_see_image": False}
    
    async def _process_guidance(self, guidance: Dict[str, Any], step: int) -> bool:
        """Process guidance from OpenAI to interact with the page."""
        action = guidance.get("action", "").strip().lower()
        self.logger.info(f"Action: {action}")
        
        try:
            if action == "enter_text":
                # Get field type information
                is_verification = guidance.get("is_verification", False)
                
                # If it's a verification code field, handle it manually
                if is_verification:
                    self.logger.info("\n=== VERIFICATION CODE REQUIRED ===")
                    self.logger.info("1. Please enter the verification code in the browser window")
                    self.logger.info("2. Complete any additional verification steps if needed")
                    self.logger.info("3. Wait until you see the next screen after verification")
                    self.logger.info("4. Then return to this terminal and press Enter to continue")
                    
                    # Wait for user to press Enter
                    input("\nPress Enter once verification is complete...")
                    
                    self.logger.info("Continuing with automation...")
                    await asyncio.sleep(2)  # Give a moment for any transitions
                    return True
                
                # Continue with normal text entry for non-verification fields
                # Get the selector and text to enter
                selector = guidance.get("selector", "").strip()
                text = guidance.get("text", "").strip()
                
                # Debug: Log all input elements on the page
                debug_elements = await self.page.evaluate("""() => {
                    const inputs = Array.from(document.querySelectorAll('input'));
                    return inputs.map(input => ({
                        placeholder: input.placeholder,
                        type: input.type,
                        id: input.id,
                        name: input.name,
                        visible: window.getComputedStyle(input).display !== 'none'
                    }));
                }""")
                self.logger.info(f"Available input elements: {debug_elements}")
                
                # Get field type information directly from OpenAI guidance
                is_username = guidance.get("is_username", False)
                is_email = guidance.get("is_email", False)
                is_password = guidance.get("is_password", False)
                
                # Determine which credential to use based on OpenAI analysis
                cred = None
                cred_type = "unknown"
                if guidance.get("is_username"):
                    cred = self.username
                    cred_type = "username"
                if guidance.get("is_email"):
                    cred = self.email
                    cred_type = "email"
                if guidance.get("is_password"):
                    cred = self.password
                    cred_type = "password"
                if guidance.get("is_verification"):
                    cred = self.verification_code
                    cred_type = "verification code"
                
                if cred is None:
                    self.logger.warning("Could not determine credential type from OpenAI guidance.")
                    return False # Cannot proceed without knowing what to type

                self.logger.info(f"Using stored {cred_type}")

                # Use the selector suggested by OpenAI
                selector = guidance.get("selector")
                if not selector:
                    self.logger.warning("No selector provided by OpenAI, attempting default selector.")
                    # Fallback logic (e.g., try a common input name)
                    selector = "input[name='text']" if guidance.get("is_email") or guidance.get("is_username") else "input[name='password']"
                
                self.logger.info(f"Using selector: {selector} to enter credential")
                
                # Log the first few characters before typing
                masked_cred = cred[:4] + "****" if cred else "[EMPTY]"
                self.logger.info(f"Attempting to type: '{masked_cred}' into selector: {selector}")

                await self._type_like_human(selector, cred)
                
                # Check if OpenAI suggests clicking a button next or just submitting
                if guidance.get("next_action") == "click_button":
                    button_selector = guidance.get("button_selector")
                    if button_selector:
                        self.logger.info(f"Clicking button with selector: {button_selector}")
                        await self.page.click(button_selector)
                    else:
                        self.logger.warning("OpenAI suggested clicking button, but no selector provided. Submitting form instead.")
                        await self.page.press(selector, "Enter")
                else:
                    self.logger.info("Form submitted by pressing Enter")
                    await self.page.press(selector, "Enter")
                
            elif action == "click_element":
                selector = guidance.get("selector", "")
                button_text = guidance.get("button_text", "")
                
                # Common Twitter login button selectors
                login_button_selectors = []
                
                # Add common login button selectors based on button text
                if button_text and button_text.lower() in ["next", "continue"]:
                    login_button_selectors = [
                        "[data-testid='LoginForm_Forward_Button']",
                        "div[role='button']:has-text('Next')",
                        "div[role='button']:has-text('Continue')",
                        "button:has-text('Next')",
                        "button:has-text('Continue')",
                        "[role='button']:has-text('Next')"
                    ]
                elif button_text and button_text.lower() in ["log in", "login", "sign in", "signin"]:
                    login_button_selectors = [
                        "[data-testid='LoginForm_Login_Button']",
                        "div[role='button']:has-text('Log in')",
                        "div[role='button']:has-text('Sign in')",
                        "button:has-text('Log in')",
                        "button:has-text('Sign in')",
                        "[role='button']:has-text('Log in')"
                    ]
                
                # Add the provided selector if it exists
                if selector and selector not in login_button_selectors:
                    login_button_selectors.insert(0, selector)
                
                # Determine human-like wait time
                human_delay = guidance.get("human_delay", 0)
                if isinstance(human_delay, (int, float)):
                    wait_time = human_delay
                elif isinstance(human_delay, str) and "-" in human_delay:
                    try:
                        min_val, max_val = map(float, human_delay.split("-"))
                        wait_time = random.uniform(min_val, max_val)
                    except:
                        wait_time = random.uniform(0.5, 2.0)
                else:
                    wait_time = random.uniform(0.5, 2.0)
                
                # Try each selector
                for btn_selector in login_button_selectors:
                    try:
                        self.logger.info(f"Trying to click button with selector: {btn_selector}")
                        element = await self.page.query_selector(btn_selector)
                        if element:
                            # Human-like pre-click pause
                            await asyncio.sleep(random.uniform(0.1, 0.5))
                            await element.click()
                            self.logger.info(f"Successfully clicked element with selector: {btn_selector}")
                            await asyncio.sleep(wait_time)
                            return True
                    except Exception as e:
                        self.logger.warning(f"Failed to click {btn_selector}: {e}")
                
                # If all selectors fail, try JavaScript approach
                self.logger.info("Trying JavaScript approach to click button")
                
                try:
                    js_params = {
                        "buttonText": button_text or "next" or "log in"
                    }
                    
                    js_code = """
                    (params) => {
                        const { buttonText } = params;
                        
                        // Function to check if element is visible
                        function isVisible(elem) {
                            if (!elem) return false;
                            const style = window.getComputedStyle(elem);
                            if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                                return false;
                            }
                            const rect = elem.getBoundingClientRect();
                            return rect.width > 0 && rect.height > 0;
                        }
                        
                        // Convert to lowercase for case-insensitive matching
                        const textToFind = buttonText.toLowerCase();
                        
                        // Try to find button by text content
                        const elements = Array.from(document.querySelectorAll('button, div[role="button"], span[role="button"], a[role="button"]'));
                        
                        for (const el of elements) {
                            if (isVisible(el) && el.textContent.toLowerCase().includes(textToFind)) {
                                el.click();
                                return true;
                            }
                        }
                        
                        // Try to find any clickable elements that might be login buttons
                        const possibleLoginButtons = Array.from(document.querySelectorAll('button, div[role="button"], input[type="submit"]'));
                        for (const btn of possibleLoginButtons) {
                            if (isVisible(btn)) {
                                btn.click();
                                return true;
                            }
                        }
                        
                        return false;
                    }
                    """
                    
                    js_result = await self.page.evaluate(js_code, js_params)
                    
                    if js_result:
                        self.logger.info(f"Successfully clicked element using JavaScript")
                        await asyncio.sleep(wait_time)
                        return True
                        
                except Exception as e:
                    self.logger.warning(f"Failed to execute JavaScript click: {e}")
                
                self.logger.warning("Failed to click any element")
                return False
                
            elif action == "wait":
                wait_time = guidance.get("wait_time", 2000)
                self.logger.info(f"Waiting for {wait_time}ms")
                await asyncio.sleep(wait_time / 1000)
                return True
                
            elif action == "press_key":
                key = guidance.get("key", "")
                if key:
                    await self.page.keyboard.press(key)
                    return True
                else:
                    self.logger.warning("Missing key for press_key action")
                    return False
                    
            elif action == "scroll":
                direction = guidance.get("direction", "down")
                distance = guidance.get("distance", 500)
                return await self._scroll_page(direction, distance)
                
            else:
                self.logger.warning(f"Unknown action: {action}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error processing guidance: {e}")
            return False
    
    async def _take_screenshot(self, name: str) -> str:
        """Take a screenshot of the current page."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.png"
        filepath = os.path.join(self.screenshots_dir, filename)
        
        await self.page.screenshot(path=filepath)
        
        self.logger.info(f"Screenshot saved to {filepath}")
        return filepath
    
    async def _get_page_text(self) -> str:
        """Get visible text from the current page."""
        try:
            # Wait for the input field to be present
            await self.page.wait_for_selector('input[type="text"]', timeout=5000)
            
            text = await self.page.evaluate("""() => {
                // Helper function to get unique text content
                const getUniqueText = (elements) => {
                    const seen = new Set();
                    return elements
                        .map(el => el.textContent.trim())
                        .filter(text => {
                            if (text && !seen.has(text)) {
                                seen.add(text);
                                return true;
                            }
                            return false;
                        })
                        .join(' | ');
                };
                
                // Get all text content (avoiding duplicates)
                const textContent = getUniqueText(
                    Array.from(document.querySelectorAll('p, h1, h2, h3, label, div[role="button"]'))
                        .filter(el => {
                            const style = window.getComputedStyle(el);
                            return style.display !== 'none' && 
                                   style.visibility !== 'hidden' && 
                                   el.textContent.trim().length > 0;
                        })
                );
                
                // Get all input fields, including those in shadow DOM
                const getInputs = (root) => {
                    const inputs = Array.from(root.querySelectorAll('input'));
                    const shadowRoots = Array.from(root.querySelectorAll('*'))
                        .map(el => el.shadowRoot)
                        .filter(Boolean);
                    
                    return inputs.concat(
                        shadowRoots.flatMap(shadowRoot => getInputs(shadowRoot))
                    );
                };
                
                const inputs = getInputs(document).map(input => ({
                    placeholder: input.placeholder,
                    type: input.type,
                    id: input.id,
                    name: input.name,
                    'aria-label': input.getAttribute('aria-label'),
                    value: input.value,
                    visible: window.getComputedStyle(input).display !== 'none'
                }));
                
                return {
                    textContent,
                    inputs
                };
            }""")
            
            # Format the content nicely
            content_parts = []
            if text.get('textContent'):
                content_parts.append("Page Text:\n" + text.get('textContent'))
            
            if text.get('inputs'):
                input_info = "Input Fields:\n" + "\n".join([
                    f"- Input: {i.get('type', 'unknown')} | " +
                    f"Placeholder: '{i.get('placeholder', 'none')}' | " +
                    f"ID: {i.get('id', 'none')} | " +
                    f"Name: {i.get('name', 'none')} | " +
                    f"Aria-Label: {i.get('aria-label', 'none')} | " +
                    f"Visible: {i.get('visible', False)}"
                    for i in text.get('inputs', [])
                ])
                content_parts.append(input_info)
            
            final_text = "\n\n".join(content_parts)
            self.logger.info(f"Page content gathered:\n{final_text}")
            return final_text
            
        except Exception as e:
            self.logger.error(f"Error getting page text: {e}")
            return ""
    
    async def _scroll_page(self, direction: str = "down", distance: int = 500) -> bool:
        """Scroll the page."""
        try:
            if direction.lower() == "up":
                distance = -abs(distance)
            else:
                distance = abs(distance)
                
            actual_distance = distance + random.randint(-50, 50)
            
            await self.page.evaluate(f"window.scrollBy(0, {actual_distance})")
            
            await asyncio.sleep(self._random_delay(0.5, 1.5))
            
            self.logger.info(f"Scrolled {direction} by {actual_distance}px")
            return True
        except Exception as e:
            self.logger.error(f"Error scrolling page: {e}")
            return False
    
    async def _check_if_logged_in(self) -> bool:
        """Check if we are logged in to Twitter."""
        try:
            logged_in_indicators = [
                "a[href='/home']",
                "a[href='/explore']",
                "a[aria-label='Home']",
                "a[aria-label='Profile']",
                "[data-testid='AppTabBar_Home_Link']",
                "[data-testid='SideNav_AccountSwitcher_Button']",
                "[aria-label='Profile']"
            ]
            
            for selector in logged_in_indicators:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        self.logger.info(f"Found logged-in indicator: {selector}")
                        return True
                except Exception:
                    continue
            
            current_url = self.page.url
            if ("/home" in current_url or 
                "/explore" in current_url or 
                "/notifications" in current_url or
                "/messages" in current_url):
                self.logger.info(f"On a logged-in page: {current_url}")
                return True
            
            return False
        except Exception as e:
            self.logger.warning(f"Error checking login status: {e}")
            return False
    
    def _prepare_login_context(self, step: int, screenshot_path: str, page_content: str = "",
                             username: str = "", verification_code: str = "") -> str:
        """Prepare context for OpenAI to assist with Twitter login."""
        # Check if we actually have page content
        has_content = bool(page_content and page_content.strip())
        self.logger.info(f"Preparing context with page content available: {has_content}")
        if has_content:
            self.logger.info(f"Content length: {len(page_content)}")
        
        return f"""I am trying to log into Twitter/X as a human user. Please analyze the screenshot and tell me what to do next.

Current step: {step}
I have the following credentials available:
- Username: {username}
- Email: {self.email}
- Password: (not shown for security)
{f'- Verification code: {verification_code}' if verification_code else ''}

Page HTML content:
{page_content}

IMPORTANT:
1. Identify the exact credentials I need to enter (username, email, password, verification code)
2. Using the HTML content above, provide the precise CSS selector that will work with the actual DOM structure
3. Make sure the selector matches what's in the HTML, not just what's visible in the screenshot

I need you to:
1. Analyze what's happening on the screen in detail
2. Tell me which credentials I need to enter
3. Provide the selector that matches the actual DOM structure shown in the HTML content

Return ONLY a valid JSON response with no comments, using this structure:
{{
  "analysis": "Simple description of what you see on screen and what needs to be done",
  "action": "enter_text/click_element/press_key/wait/scroll",
  "selector": "The selector that matches the actual DOM structure in the HTML content",
  "is_username": true/false,
  "is_email": true/false,
  "is_password": true/false,
  "is_verification": true/false,
  "can_see_image_provided": true/false,
  "can_see_html_body_provided": {str(has_content).lower()}
}}

Pay close attention to the actual HTML structure and DOM elements to provide a selector that will work with the real page structure.
"""
    
    def _random_delay(self, min_seconds: float, max_seconds: float) -> float:
        """Generate a random delay between min and max seconds."""
        return random.uniform(min_seconds, max_seconds)
    
    def _save_output(self, data: Dict[str, Any]) -> None:
        """Save output to file."""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
            self.logger.info(f"Saved extraction results to {self.output_file}")
        except Exception as e:
            self.logger.error(f"Error saving output: {e}")
    
    async def _show_verification_prompt(self) -> None:
        """Show a message to the user about the verification code."""
        try:
            # Take a screenshot
            screenshot_path = await self._take_screenshot("verification_prompt")
            self.logger.info(f"Twitter is asking for a verification code. See screenshot: {screenshot_path}")
            
            # Display the screenshot if possible
            if self.headless:
                # If running headless, try to show the image using system tools
                if os.name == 'posix':  # macOS or Linux
                    try:
                        import subprocess
                        subprocess.Popen(['open', screenshot_path])
                    except:
                        pass
                elif os.name == 'nt':  # Windows
                    try:
                        import subprocess
                        subprocess.Popen(['start', screenshot_path], shell=True)
                    except:
                        pass
        except Exception as e:
            self.logger.error(f"Error showing verification prompt: {e}")
    
    async def _get_field_placeholder(self) -> str:
        """Get the placeholder or aria-label of the current visible input field."""
        try:
            placeholder = await self.page.evaluate("""
                () => {
                    const inputs = Array.from(document.querySelectorAll('input:not([type="hidden"])'));
                    for (const input of inputs) {
                        if (input.offsetWidth > 0 && input.offsetHeight > 0) {
                            return (input.placeholder || input.getAttribute('aria-label') || 
                                   input.getAttribute('name') || input.id || '');
                        }
                    }
                    return '';
                }
            """)
            return placeholder
        except Exception as e:
            self.logger.error(f"Error getting field placeholder: {e}")
            return ""
    
    async def _type_like_human(self, selector: str, text: str) -> None:
        """Type text in a human-like manner with realistic variations."""
        try:
            # Base typing speed ranges (in seconds)
            FAST_CHAR_DELAY = (0.01, 0.07)  # Fast typing
            SLOW_CHAR_DELAY = (0.08, 0.16)  # Slow typing
            MISTAKE_PROBABILITY = 0.03  # 3% chance of typo
            PAUSE_PROBABILITY = 0.02    # 2% chance of pause between characters
            
            self.logger.info(f"Starting human-like typing for text of length {len(text)}")
            
            for i, char in enumerate(text):
                # Occasionally make a mistake and correct it
                if random.random() < MISTAKE_PROBABILITY:
                    # Type a wrong character
                    wrong_char = random.choice('qwertyuiopasdfghjklzxcvbnm')
                    await self.page.type(selector, wrong_char)
                    await asyncio.sleep(random.uniform(0.1, 0.3))
                    
                    # Delete the wrong character
                    await self.page.keyboard.press('Backspace')
                    await asyncio.sleep(random.uniform(0.1, 0.2))
                
                # Type the correct character
                await self.page.type(selector, char)
                
                # Determine typing speed based on context
                if char in '.,!?':
                    # Pause longer after punctuation
                    await asyncio.sleep(random.uniform(0.3, 0.7))
                elif char == ' ':
                    # Slight pause after words
                    await asyncio.sleep(random.uniform(0.05, 0.15))
                elif i > 0 and text[i-1] in 'qwertyuiopasdfghjklzxcvbnm' and char in 'qwertyuiopasdfghjklzxcvbnm':
                    # Fast typing for consecutive letters
                    await asyncio.sleep(random.uniform(*FAST_CHAR_DELAY))
                else:
                    # Slower typing for other characters
                    await asyncio.sleep(random.uniform(*SLOW_CHAR_DELAY))
                
                # Occasionally add a longer pause (like thinking or distraction)
                if random.random() < PAUSE_PROBABILITY:
                    await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # Natural pause after finishing typing
            await asyncio.sleep(random.uniform(0.3, 0.7))
            
        except Exception as e:
            self.logger.error(f"Error during human-like typing: {e}")
            raise

    async def _inject_enhanced_stealth_scripts(self) -> None:
        """Inject enhanced stealth scripts based on ZenRows recommendations."""
        if self.page:
            await self.page.add_init_script("""
                (() => {
                    // WebGL fingerprint
                    const getParameter = WebGLRenderingContext.prototype.getParameter;
                    WebGLRenderingContext.prototype.getParameter = function(parameter) {
                        if (parameter === 37445) {
                            return 'Intel Inc.';
                        }
                        if (parameter === 37446) {
                            return 'Intel Iris OpenGL Engine';
                        }
                        return getParameter.apply(this, arguments);
                    };

                    // Platform
                    Object.defineProperty(navigator, 'platform', {
                        get: () => 'MacIntel'
                    });

                    // Languages
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-AU', 'en']
                    });

                    // Plugins (more realistic set)
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [
                            {
                                0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                                description: "Portable Document Format",
                                filename: "internal-pdf-viewer",
                                length: 1,
                                name: "Chrome PDF Plugin"
                            },
                            {
                                0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format"},
                                description: "Portable Document Format",
                                filename: "internal-pdf-viewer",
                                length: 1,
                                name: "Chrome PDF Viewer"
                            },
                            {
                                0: {type: "application/x-nacl", suffixes: "", description: "Native Client Executable"},
                                description: "Native Client Executable",
                                filename: "internal-nacl-plugin",
                                length: 1,
                                name: "Native Client"
                            }
                        ]
                    });

                    // Mask Playwright-specific properties
                    delete window.playwright;
                    delete window.Playwright;

                    // WebDriver
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false
                    });

                    // Permissions
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({state: Notification.permission}) :
                            originalQuery(parameters)
                    );

                    // Automation flags
                    window.navigator.chrome = {
                        runtime: {}
                    };

                    // Additional navigator properties
                    Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
                    Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});

                    // Connection
                    Object.defineProperty(navigator, 'connection', {
                        get: () => ({
                            effectiveType: '4g',
                            rtt: 50,
                            downlink: 10,
                            saveData: false
                        })
                    });
                })();
            """)

            # Set enhanced headers
            await self.page.set_extra_http_headers({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-AU,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Ch-Ua": '"Chromium";v="122", "Google Chrome";v="122"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"macOS"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1"
            })

    def fetch(self):
        """
        Synchronous wrapper for the async extract method.
        This is needed to comply with the ETL pipeline interface.
        """
        import asyncio
        try:
            # Create new event loop if there isn't one
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Run the async extract method
            return loop.run_until_complete(self.extract())
        except Exception as e:
            self.logger.error(f"Error in fetch: {e}")
            raise
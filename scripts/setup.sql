/*
 * ===========================================================================
 * Sequent™ - Complete Setup 
 * ===========================================================================
 */

-- Set query tag for tracking
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"customer_journey_analytics_with_sequent","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';

-- ===========================================================================
-- CONFIGURATION
-- ===========================================================================

SET JOURNEY_COUNT = 100000;  -- 100K journeys = ~1-2M events (adjust as needed)

-- ===========================================================================
-- SECTION 1: INFRASTRUCTURE
-- ===========================================================================

USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Capture current user for role assignment
SET USERNAME = (SELECT CURRENT_USER());

-- Create dedicated role for the demo
CREATE ROLE IF NOT EXISTS SEQUENT_ROLE;

-- Grant role to current user running the setup
GRANT ROLE SEQUENT_ROLE TO USER identifier($USERNAME);

-- Create warehouse (Snowpark-optimized for ML and advanced analytics)
CREATE WAREHOUSE IF NOT EXISTS SEQUENT_WH
  WITH 
  WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = FALSE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 2
  SCALING_POLICY = 'STANDARD';

GRANT USAGE ON WAREHOUSE SEQUENT_WH TO ROLE SEQUENT_ROLE;
GRANT OPERATE ON WAREHOUSE SEQUENT_WH TO ROLE SEQUENT_ROLE;

USE WAREHOUSE SEQUENT_WH;

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS SEQUENT_DB;
GRANT ALL ON DATABASE SEQUENT_DB TO ROLE SEQUENT_ROLE;

CREATE SCHEMA IF NOT EXISTS SEQUENT_DB.RETAIL;
CREATE SCHEMA IF NOT EXISTS SEQUENT_DB.GAMING;
CREATE SCHEMA IF NOT EXISTS SEQUENT_DB.HOSPITALITY;
CREATE SCHEMA IF NOT EXISTS SEQUENT_DB.DELIVERY;
CREATE SCHEMA IF NOT EXISTS SEQUENT_DB.FSI;
CREATE SCHEMA IF NOT EXISTS SEQUENT_DB.ANALYTICS;

GRANT ALL ON ALL SCHEMAS IN DATABASE SEQUENT_DB TO ROLE SEQUENT_ROLE;

-- ===========================================================================
-- SECTION 2: PRIVILEGES
-- ===========================================================================

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SEQUENT_ROLE;

GRANT CREATE TABLE ON SCHEMA SEQUENT_DB.RETAIL TO ROLE SEQUENT_ROLE;
GRANT CREATE TABLE ON SCHEMA SEQUENT_DB.GAMING TO ROLE SEQUENT_ROLE;
GRANT CREATE TABLE ON SCHEMA SEQUENT_DB.HOSPITALITY TO ROLE SEQUENT_ROLE;
GRANT CREATE TABLE ON SCHEMA SEQUENT_DB.DELIVERY TO ROLE SEQUENT_ROLE;
GRANT CREATE TABLE ON SCHEMA SEQUENT_DB.FSI TO ROLE SEQUENT_ROLE;
GRANT CREATE TABLE ON SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;

GRANT CREATE VIEW ON SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;
GRANT CREATE STAGE ON SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;
GRANT CREATE STREAMLIT ON SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;

-- Future grants (critical for app functionality)
GRANT ALL ON FUTURE TABLES IN SCHEMA SEQUENT_DB.RETAIL TO ROLE SEQUENT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA SEQUENT_DB.GAMING TO ROLE SEQUENT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA SEQUENT_DB.HOSPITALITY TO ROLE SEQUENT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA SEQUENT_DB.DELIVERY TO ROLE SEQUENT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA SEQUENT_DB.FSI TO ROLE SEQUENT_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;
GRANT ALL ON FUTURE VIEWS IN SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;

-- ===========================================================================
-- SECTION 3: RETAIL SYNTHETIC DATA FUNCTION (Example)
-- ===========================================================================

USE SCHEMA SEQUENT_DB.RETAIL;

CREATE OR REPLACE FUNCTION generate_ecommerce_journey()
RETURNS TABLE (
    -- Core identifiers
    event_id STRING,
    visitor_id STRING,
    customer_id STRING,
    
    -- Event details
    event_timestamp TIMESTAMP,
    event_type STRING,
    event_category STRING,
    event_action STRING,
    event_label STRING,
    
    -- Page/Screen information (Adobe Analytics style)
    page_name STRING,
    page_url STRING,
    page_type STRING,
    site_section STRING,
    referrer_url STRING,
    
    -- Technical details
    browser STRING,
    browser_version STRING,
    operating_system STRING,
    device_type STRING,
    screen_resolution STRING,
    user_agent STRING,
    ip_address STRING,
    
    -- Geographic data
    country STRING,
    state STRING,
    city STRING,
    zip_code STRING,
    
    -- Page interaction details
    time_on_page INT,
    scroll_depth INT,
    clicks_on_page INT,
    
    -- Ecommerce specific fields
    product_name STRING,
    product_category STRING,
    product_brand STRING,
    product_price DECIMAL(10,2),
    order_value DECIMAL(12,2),
    quantity INT,
    discount_amount DECIMAL(10,2),
    payment_method STRING,
    shipping_method STRING,
    
    -- Campaign/Marketing (Adobe Analytics style)
    campaign_id STRING,
    traffic_source STRING,
    medium STRING,
    referrer_domain STRING,
    
    -- Custom dimensions with explicit names
    customer_segment STRING,
    customer_lifetime_value_tier STRING,
    sport_preference STRING,
    size_preference STRING,
    loyalty_program_member STRING,
    
    -- Custom events with explicit names
    product_views INT,
    add_to_cart_events INT,
    purchase_events INT,
    search_events INT,
    wishlist_additions INT,
    
    -- Additional context
    is_mobile_app BOOLEAN,
    page_load_time_ms INT,
    conversion_flag BOOLEAN,
    revenue_impact DECIMAL(12,2)
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'generateJourney'
PACKAGES = ('faker')
AS $$
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

class generateJourney:
    def process(self):
        shared_events = {
            'entry_points': ['homepage_visit', 'category_landing', 'search_result_click', 'email_campaign_click', 'social_media_click', 'mobile_app_open', 'influencer_link_click', 'google_ads_click', 'affiliate_referral', 'direct_url_entry'],
            'authentication': ['login_attempt', 'login_success', 'guest_checkout_start', 'account_creation_start', 'password_reset_request', 'social_login_attempt', 'email_verification'],
            'product_discovery': ['category_browse', 'product_listing_view', 'filter_application', 'sort_selection', 'search_query', 'search_refinement', 'brand_page_visit', 'sale_section_browse', 'new_arrivals_view', 'trending_products_view', 'size_guide_view'],
            'product_interaction': ['product_detail_view', 'product_image_zoom', 'size_selection', 'color_selection', 'product_video_play', 'review_section_view', 'qa_section_view', 'size_chart_view', 'product_comparison', 'related_products_view', 'recently_viewed_check'],
            'cart_interactions': ['add_to_cart', 'cart_view', 'quantity_update', 'remove_from_cart', 'save_for_later', 'cart_abandonment_recovery', 'promo_code_entry', 'shipping_calculator_use', 'cart_share'],
            'checkout_process': ['checkout_initiation', 'shipping_info_entry', 'billing_info_entry', 'payment_method_selection', 'order_review', 'purchase_completion', 'order_confirmation_view', 'receipt_email_open'],
            'account_management': ['account_dashboard_view', 'order_history_view', 'profile_update', 'address_book_management', 'payment_methods_management', 'preferences_update', 'subscription_management', 'loyalty_points_check'],
            'wishlist_favorites': ['wishlist_view', 'add_to_wishlist', 'remove_from_wishlist', 'wishlist_share', 'move_to_cart_from_wishlist', 'favorites_organization'],
            'reviews_social': ['review_submission', 'review_reading', 'rating_submission', 'photo_review_upload', 'social_share', 'referral_program_use', 'user_generated_content_view'],
            'support_touchpoints': ['help_center_visit', 'faq_browse', 'live_chat_initiate', 'contact_form_submission', 'return_policy_view', 'shipping_info_view', 'size_exchange_request', 'order_tracking', 'customer_service_call', 'chatbot_interaction'],
            'mobile_specific': ['app_notification_click', 'barcode_scan', 'store_locator_use', 'mobile_exclusive_offer_view', 'push_notification_settings', 'mobile_payment_setup', 'offline_wishlist_sync'],
            'promotional': ['coupon_code_search', 'sale_banner_click', 'loyalty_program_join', 'email_signup', 'sms_signup', 'flash_sale_participation', 'seasonal_promotion_view', 'bundle_offer_view'],
            'cross_selling': ['recommended_products_view', 'frequently_bought_together', 'personalized_recommendations', 'category_upsell_view', 'accessory_suggestions', 'outfit_completion_suggestions'],
            'exits': ['logout', 'session_timeout', 'navigation_away', 'app_background', 'browser_close', 'checkout_abandonment']
        }
        
        journey_templates = {
            'new_customer_exploration': {'primary_goal': 'first_purchase', 'base_flow': [('entry_points', 1), ('product_discovery', random.randint(3, 6)), ('product_interaction', random.randint(2, 4)), ('authentication', random.randint(1, 2)), ('cart_interactions', random.randint(1, 3)), ('checkout_process', random.randint(2, 6)), ('exits', 1)], 'conversion_rate': 0.15, 'revenue_range': (25, 200)},
            'returning_customer_purchase': {'primary_goal': 'repeat_purchase', 'base_flow': [('entry_points', 1), ('authentication', 1), ('product_discovery', random.randint(1, 3)), ('product_interaction', random.randint(1, 3)), ('cart_interactions', random.randint(1, 2)), ('checkout_process', random.randint(3, 5)), ('account_management', random.randint(0, 1)), ('exits', 1)], 'conversion_rate': 0.35, 'revenue_range': (40, 300)},
            'athletic_gear_research': {'primary_goal': 'product_research', 'base_flow': [('entry_points', 1), ('product_discovery', random.randint(4, 7)), ('product_interaction', random.randint(3, 6)), ('reviews_social', random.randint(1, 3)), ('support_touchpoints', random.randint(0, 2)), ('wishlist_favorites', random.randint(0, 2)), ('exits', 1)], 'conversion_rate': 0.08, 'revenue_range': (0, 0)},
            'seasonal_shopping_spree': {'primary_goal': 'bulk_purchase', 'base_flow': [('entry_points', 1), ('promotional', random.randint(1, 2)), ('product_discovery', random.randint(3, 5)), ('product_interaction', random.randint(4, 8)), ('cart_interactions', random.randint(2, 4)), ('cross_selling', random.randint(1, 3)), ('checkout_process', random.randint(3, 6)), ('exits', 1)], 'conversion_rate': 0.45, 'revenue_range': (150, 800)},
            'mobile_app_browsing': {'primary_goal': 'mobile_engagement', 'base_flow': [('entry_points', 1), ('mobile_specific', random.randint(2, 4)), ('product_discovery', random.randint(2, 4)), ('product_interaction', random.randint(1, 3)), ('wishlist_favorites', random.randint(0, 2)), ('cart_interactions', random.randint(0, 2)), ('exits', 1)], 'conversion_rate': 0.12, 'revenue_range': (20, 150)},
            'gift_shopping_journey': {'primary_goal': 'gift_purchase', 'base_flow': [('entry_points', 1), ('product_discovery', random.randint(2, 4)), ('product_interaction', random.randint(2, 5)), ('reviews_social', random.randint(1, 2)), ('support_touchpoints', random.randint(0, 2)), ('cart_interactions', random.randint(1, 3)), ('checkout_process', random.randint(3, 6)), ('exits', 1)], 'conversion_rate': 0.28, 'revenue_range': (30, 250)},
            'loyalty_member_shopping': {'primary_goal': 'loyalty_purchase', 'base_flow': [('entry_points', 1), ('authentication', 1), ('account_management', random.randint(1, 2)), ('promotional', random.randint(1, 2)), ('product_discovery', random.randint(2, 4)), ('product_interaction', random.randint(1, 3)), ('cart_interactions', random.randint(1, 2)), ('checkout_process', random.randint(2, 4)), ('exits', 1)], 'conversion_rate': 0.55, 'revenue_range': (60, 400)},
            'support_interaction': {'primary_goal': 'customer_service', 'base_flow': [('entry_points', 1), ('authentication', random.randint(0, 1)), ('account_management', random.randint(1, 2)), ('support_touchpoints', random.randint(3, 6)), ('product_discovery', random.randint(0, 2)), ('exits', 1)], 'conversion_rate': 0.20, 'revenue_range': (0, 100)},
            'cart_abandonment_recovery': {'primary_goal': 'abandoned_cart_return', 'base_flow': [('entry_points', 1), ('authentication', random.randint(0, 1)), ('cart_interactions', random.randint(1, 3)), ('promotional', random.randint(0, 1)), ('checkout_process', random.randint(0, 4)), ('exits', 1)], 'conversion_rate': 0.25, 'revenue_range': (25, 180)},
            'social_media_influenced': {'primary_goal': 'social_conversion', 'base_flow': [('entry_points', 1), ('product_interaction', random.randint(2, 4)), ('reviews_social', random.randint(1, 3)), ('wishlist_favorites', random.randint(0, 2)), ('cart_interactions', random.randint(0, 2)), ('checkout_process', random.randint(0, 5)), ('exits', 1)], 'conversion_rate': 0.18, 'revenue_range': (35, 220)}
        }
        
        event_details = {
            'homepage_visit': {'name': 'Homepage', 'url': '/', 'type': 'marketing', 'category': 'navigation'},
            'category_landing': {'name': 'Category Landing', 'url': '/category/running-shoes', 'type': 'marketing', 'category': 'navigation'},
            'search_result_click': {'name': 'Search Results', 'url': '/search?q=nike', 'type': 'search', 'category': 'search'},
            'email_campaign_click': {'name': 'Email Campaign', 'url': '/campaign/new-arrivals', 'type': 'marketing', 'category': 'campaign'},
            'social_media_click': {'name': 'Social Media', 'url': '/social-landing', 'type': 'marketing', 'category': 'social'},
            'mobile_app_open': {'name': 'Mobile App Home', 'url': '/app/home', 'type': 'mobile', 'category': 'mobile'},
            'influencer_link_click': {'name': 'Influencer Link', 'url': '/influencer/athlete-gear', 'type': 'marketing', 'category': 'influencer'},
            'google_ads_click': {'name': 'Google Ads', 'url': '/ads-landing', 'type': 'marketing', 'category': 'paid_search'},
            'affiliate_referral': {'name': 'Affiliate Referral', 'url': '/affiliate/sports-blog', 'type': 'marketing', 'category': 'affiliate'},
            'direct_url_entry': {'name': 'Direct Entry', 'url': '/direct', 'type': 'direct', 'category': 'direct'},
            'login_attempt': {'name': 'Login Page', 'url': '/login', 'type': 'authentication', 'category': 'auth'},
            'login_success': {'name': 'Login Success', 'url': '/my-account', 'type': 'authentication', 'category': 'auth'},
            'guest_checkout_start': {'name': 'Guest Checkout', 'url': '/checkout/guest', 'type': 'checkout', 'category': 'auth'},
            'account_creation_start': {'name': 'Create Account', 'url': '/register', 'type': 'authentication', 'category': 'auth'},
            'password_reset_request': {'name': 'Password Reset', 'url': '/forgot-password', 'type': 'authentication', 'category': 'auth'},
            'social_login_attempt': {'name': 'Social Login', 'url': '/login/social', 'type': 'authentication', 'category': 'auth'},
            'email_verification': {'name': 'Email Verification', 'url': '/verify-email', 'type': 'authentication', 'category': 'auth'},
            'category_browse': {'name': 'Category Browse', 'url': '/category/athletic-wear', 'type': 'product_listing', 'category': 'browse'},
            'product_listing_view': {'name': 'Product Listing', 'url': '/products/running-shoes', 'type': 'product_listing', 'category': 'browse'},
            'filter_application': {'name': 'Apply Filters', 'url': '/products/shoes?filter=brand:nike', 'type': 'product_listing', 'category': 'filter'},
            'sort_selection': {'name': 'Sort Products', 'url': '/products/shoes?sort=price_low', 'type': 'product_listing', 'category': 'sort'},
            'search_query': {'name': 'Search', 'url': '/search?q=basketball', 'type': 'search', 'category': 'search'},
            'search_refinement': {'name': 'Search Refinement', 'url': '/search?q=basketball+shoes', 'type': 'search', 'category': 'search'},
            'brand_page_visit': {'name': 'Brand Page', 'url': '/brand/adidas', 'type': 'marketing', 'category': 'brand'},
            'sale_section_browse': {'name': 'Sale Section', 'url': '/sale', 'type': 'marketing', 'category': 'promotion'},
            'new_arrivals_view': {'name': 'New Arrivals', 'url': '/new-arrivals', 'type': 'marketing', 'category': 'browse'},
            'trending_products_view': {'name': 'Trending Products', 'url': '/trending', 'type': 'marketing', 'category': 'browse'},
            'size_guide_view': {'name': 'Size Guide', 'url': '/size-guide', 'type': 'support', 'category': 'guide'},
            'product_detail_view': {'name': 'Product Details', 'url': '/product/nike-air-max-270', 'type': 'product_detail', 'category': 'product'},
            'product_image_zoom': {'name': 'Image Zoom', 'url': '/product/nike-air-max-270#gallery', 'type': 'product_detail', 'category': 'product'},
            'size_selection': {'name': 'Size Selection', 'url': '/product/nike-air-max-270#size', 'type': 'product_detail', 'category': 'product'},
            'color_selection': {'name': 'Color Selection', 'url': '/product/nike-air-max-270#color', 'type': 'product_detail', 'category': 'product'},
            'product_video_play': {'name': 'Product Video', 'url': '/product/nike-air-max-270#video', 'type': 'product_detail', 'category': 'media'},
            'review_section_view': {'name': 'Product Reviews', 'url': '/product/nike-air-max-270#reviews', 'type': 'product_detail', 'category': 'social_proof'},
            'qa_section_view': {'name': 'Q&A Section', 'url': '/product/nike-air-max-270#qa', 'type': 'product_detail', 'category': 'support'},
            'size_chart_view': {'name': 'Size Chart', 'url': '/product/nike-air-max-270#size-chart', 'type': 'product_detail', 'category': 'guide'},
            'product_comparison': {'name': 'Product Comparison', 'url': '/compare', 'type': 'tools', 'category': 'comparison'},
            'related_products_view': {'name': 'Related Products', 'url': '/product/nike-air-max-270#related', 'type': 'product_detail', 'category': 'cross_sell'},
            'recently_viewed_check': {'name': 'Recently Viewed', 'url': '/recently-viewed', 'type': 'account', 'category': 'personalization'},
            'add_to_cart': {'name': 'Add to Cart', 'url': '/cart/add', 'type': 'ecommerce', 'category': 'cart'},
            'cart_view': {'name': 'Shopping Cart', 'url': '/cart', 'type': 'ecommerce', 'category': 'cart'},
            'quantity_update': {'name': 'Update Quantity', 'url': '/cart/update', 'type': 'ecommerce', 'category': 'cart'},
            'remove_from_cart': {'name': 'Remove from Cart', 'url': '/cart/remove', 'type': 'ecommerce', 'category': 'cart'},
            'save_for_later': {'name': 'Save for Later', 'url': '/cart/save-later', 'type': 'ecommerce', 'category': 'cart'},
            'cart_abandonment_recovery': {'name': 'Cart Recovery', 'url': '/cart/recovery', 'type': 'ecommerce', 'category': 'recovery'},
            'promo_code_entry': {'name': 'Promo Code', 'url': '/cart/promo', 'type': 'ecommerce', 'category': 'promotion'},
            'shipping_calculator_use': {'name': 'Shipping Calculator', 'url': '/cart/shipping', 'type': 'tools', 'category': 'shipping'},
            'cart_share': {'name': 'Share Cart', 'url': '/cart/share', 'type': 'social', 'category': 'sharing'},
            'checkout_initiation': {'name': 'Start Checkout', 'url': '/checkout', 'type': 'ecommerce', 'category': 'checkout'},
            'shipping_info_entry': {'name': 'Shipping Info', 'url': '/checkout/shipping', 'type': 'ecommerce', 'category': 'checkout'},
            'billing_info_entry': {'name': 'Billing Info', 'url': '/checkout/billing', 'type': 'ecommerce', 'category': 'checkout'},
            'payment_method_selection': {'name': 'Payment Method', 'url': '/checkout/payment', 'type': 'ecommerce', 'category': 'checkout'},
            'order_review': {'name': 'Order Review', 'url': '/checkout/review', 'type': 'ecommerce', 'category': 'checkout'},
            'purchase_completion': {'name': 'Purchase Complete', 'url': '/checkout/complete', 'type': 'ecommerce', 'category': 'purchase'},
            'order_confirmation_view': {'name': 'Order Confirmation', 'url': '/order/confirmation', 'type': 'ecommerce', 'category': 'confirmation'},
            'receipt_email_open': {'name': 'Receipt Email', 'url': '/email/receipt', 'type': 'email', 'category': 'confirmation'},
            'account_dashboard_view': {'name': 'Account Dashboard', 'url': '/my-account', 'type': 'account', 'category': 'account'},
            'order_history_view': {'name': 'Order History', 'url': '/my-account/orders', 'type': 'account', 'category': 'account'},
            'profile_update': {'name': 'Update Profile', 'url': '/my-account/profile', 'type': 'account', 'category': 'account'},
            'address_book_management': {'name': 'Address Book', 'url': '/my-account/addresses', 'type': 'account', 'category': 'account'},
            'payment_methods_management': {'name': 'Payment Methods', 'url': '/my-account/payments', 'type': 'account', 'category': 'account'},
            'preferences_update': {'name': 'Preferences', 'url': '/my-account/preferences', 'type': 'account', 'category': 'account'},
            'subscription_management': {'name': 'Subscriptions', 'url': '/my-account/subscriptions', 'type': 'account', 'category': 'subscription'},
            'loyalty_points_check': {'name': 'Loyalty Points', 'url': '/my-account/loyalty', 'type': 'account', 'category': 'loyalty'},
            'wishlist_view': {'name': 'Wishlist', 'url': '/wishlist', 'type': 'wishlist', 'category': 'wishlist'},
            'add_to_wishlist': {'name': 'Add to Wishlist', 'url': '/wishlist/add', 'type': 'wishlist', 'category': 'wishlist'},
            'remove_from_wishlist': {'name': 'Remove from Wishlist', 'url': '/wishlist/remove', 'type': 'wishlist', 'category': 'wishlist'},
            'wishlist_share': {'name': 'Share Wishlist', 'url': '/wishlist/share', 'type': 'social', 'category': 'sharing'},
            'move_to_cart_from_wishlist': {'name': 'Wishlist to Cart', 'url': '/wishlist/move-to-cart', 'type': 'wishlist', 'category': 'conversion'},
            'favorites_organization': {'name': 'Organize Favorites', 'url': '/wishlist/organize', 'type': 'wishlist', 'category': 'organization'},
            'review_submission': {'name': 'Submit Review', 'url': '/review/submit', 'type': 'social', 'category': 'review'},
            'review_reading': {'name': 'Read Reviews', 'url': '/reviews', 'type': 'social', 'category': 'social_proof'},
            'rating_submission': {'name': 'Submit Rating', 'url': '/rating/submit', 'type': 'social', 'category': 'rating'},
            'photo_review_upload': {'name': 'Photo Review', 'url': '/review/photo', 'type': 'social', 'category': 'ugc'},
            'social_share': {'name': 'Social Share', 'url': '/share', 'type': 'social', 'category': 'sharing'},
            'referral_program_use': {'name': 'Referral Program', 'url': '/referral', 'type': 'marketing', 'category': 'referral'},
            'user_generated_content_view': {'name': 'User Content', 'url': '/community', 'type': 'social', 'category': 'ugc'},
            'help_center_visit': {'name': 'Help Center', 'url': '/help', 'type': 'support', 'category': 'support'},
            'faq_browse': {'name': 'FAQ', 'url': '/faq', 'type': 'support', 'category': 'support'},
            'live_chat_initiate': {'name': 'Live Chat', 'url': '/chat', 'type': 'support', 'category': 'support'},
            'contact_form_submission': {'name': 'Contact Form', 'url': '/contact', 'type': 'support', 'category': 'support'},
            'return_policy_view': {'name': 'Return Policy', 'url': '/returns', 'type': 'support', 'category': 'policy'},
            'shipping_info_view': {'name': 'Shipping Info', 'url': '/shipping', 'type': 'support', 'category': 'policy'},
            'size_exchange_request': {'name': 'Size Exchange', 'url': '/exchange', 'type': 'support', 'category': 'returns'},
            'order_tracking': {'name': 'Track Order', 'url': '/track', 'type': 'support', 'category': 'tracking'},
            'customer_service_call': {'name': 'Customer Service', 'url': '/support/call', 'type': 'support', 'category': 'phone'},
            'chatbot_interaction': {'name': 'Chatbot', 'url': '/chatbot', 'type': 'support', 'category': 'automation'},
            'app_notification_click': {'name': 'App Notification', 'url': '/app/notification', 'type': 'mobile', 'category': 'notification'},
            'barcode_scan': {'name': 'Barcode Scan', 'url': '/app/scan', 'type': 'mobile', 'category': 'scan'},
            'store_locator_use': {'name': 'Store Locator', 'url': '/stores', 'type': 'mobile', 'category': 'location'},
            'mobile_exclusive_offer_view': {'name': 'Mobile Offer', 'url': '/app/exclusive', 'type': 'mobile', 'category': 'promotion'},
            'push_notification_settings': {'name': 'Push Settings', 'url': '/app/settings/notifications', 'type': 'mobile', 'category': 'settings'},
            'mobile_payment_setup': {'name': 'Mobile Payment', 'url': '/app/payment', 'type': 'mobile', 'category': 'payment'},
            'offline_wishlist_sync': {'name': 'Offline Sync', 'url': '/app/sync', 'type': 'mobile', 'category': 'sync'},
            'coupon_code_search': {'name': 'Coupon Search', 'url': '/coupons', 'type': 'promotion', 'category': 'promotion'},
            'sale_banner_click': {'name': 'Sale Banner', 'url': '/sale/flash', 'type': 'promotion', 'category': 'promotion'},
            'loyalty_program_join': {'name': 'Join Loyalty', 'url': '/loyalty/join', 'type': 'loyalty', 'category': 'loyalty'},
            'email_signup': {'name': 'Email Signup', 'url': '/newsletter', 'type': 'marketing', 'category': 'signup'},
            'sms_signup': {'name': 'SMS Signup', 'url': '/sms-alerts', 'type': 'marketing', 'category': 'signup'},
            'flash_sale_participation': {'name': 'Flash Sale', 'url': '/flash-sale', 'type': 'promotion', 'category': 'flash_sale'},
            'seasonal_promotion_view': {'name': 'Seasonal Sale', 'url': '/seasonal', 'type': 'promotion', 'category': 'seasonal'},
            'bundle_offer_view': {'name': 'Bundle Offer', 'url': '/bundles', 'type': 'promotion', 'category': 'bundle'},
            'recommended_products_view': {'name': 'Recommended Products', 'url': '/recommendations', 'type': 'personalization', 'category': 'recommendation'},
            'frequently_bought_together': {'name': 'Frequently Bought Together', 'url': '/product/bundle', 'type': 'cross_sell', 'category': 'bundle'},
            'personalized_recommendations': {'name': 'Personalized Recs', 'url': '/for-you', 'type': 'personalization', 'category': 'personalization'},
            'category_upsell_view': {'name': 'Category Upsell', 'url': '/category/premium', 'type': 'upsell', 'category': 'upsell'},
            'accessory_suggestions': {'name': 'Accessory Suggestions', 'url': '/accessories', 'type': 'cross_sell', 'category': 'accessories'},
            'outfit_completion_suggestions': {'name': 'Complete the Look', 'url': '/outfit-builder', 'type': 'cross_sell', 'category': 'styling'},
            'logout': {'name': 'Logout', 'url': '/logout', 'type': 'authentication', 'category': 'exit'},
            'session_timeout': {'name': 'Session Timeout', 'url': '/timeout', 'type': 'system', 'category': 'exit'},
            'navigation_away': {'name': 'Navigate Away', 'url': '/external', 'type': 'system', 'category': 'exit'},
            'app_background': {'name': 'App Background', 'url': '/app/background', 'type': 'mobile', 'category': 'exit'},
            'browser_close': {'name': 'Browser Close', 'url': '/close', 'type': 'system', 'category': 'exit'},
            'checkout_abandonment': {'name': 'Checkout Abandonment', 'url': '/checkout/abandon', 'type': 'ecommerce', 'category': 'abandonment'}
        }
        
        product_names = ['Nike Air Max 270', 'Adidas Ultraboost 22', 'Under Armour HOVR Phantom', 'New Balance Fresh Foam X', 'ASICS Gel-Kayano 29', 'Brooks Ghost 15', 'Nike Dri-FIT Training Shirt', 'Adidas Climalite Tank Top', 'Lululemon Swiftly Tech', 'Under Armour HeatGear Leggings', 'Nike Pro Shorts', 'Adidas 3-Stripes Track Pants', 'Patagonia Better Sweater', 'The North Face Venture Jacket', 'Columbia Flash Forward Windbreaker', 'Wilson Tennis Racket Pro Staff', 'Spalding Basketball Official Size', 'Callaway Golf Driver', 'Yeti Water Bottle Rambler', 'Hydro Flask Standard Mouth', 'Nike Training Gloves', 'Fitbit Charge 5', 'Apple Watch Series 8', 'Garmin Forerunner 955']
        product_categories = ['running_shoes', 'training_shoes', 'basketball_shoes', 'tennis_shoes', 'athletic_tops', 'athletic_bottoms', 'outerwear', 'swimwear', 'team_sports', 'outdoor_recreation', 'fitness_accessories', 'technology']
        product_brands = ['Nike', 'Adidas', 'Under Armour', 'New Balance', 'ASICS', 'Brooks', 'Lululemon', 'Patagonia', 'The North Face', 'Columbia', 'Reebok', 'Puma', 'Wilson', 'Spalding', 'Callaway', 'Yeti', 'Hydro Flask']
        customer_segments = ['casual_fitness', 'serious_athlete', 'weekend_warrior', 'team_sports_player', 'outdoor_enthusiast', 'fashion_focused', 'budget_conscious', 'premium_buyer']
        sport_preferences = ['running', 'basketball', 'tennis', 'soccer', 'golf', 'hiking', 'yoga', 'crossfit', 'swimming', 'cycling', 'football', 'baseball']
        size_preferences = ['XS', 'S', 'M', 'L', 'XL', 'XXL', '6', '7', '8', '9', '10', '11', '12']
        payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'klarna', 'afterpay']
        shipping_methods = ['standard', 'express', 'overnight', 'store_pickup', 'curbside_pickup']
        browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Mobile Safari', 'Chrome Mobile', 'Samsung Internet']
        browser_versions = ['120.0', '119.0', '118.0', '117.0', '116.0', '115.0']
        operating_systems = ['Windows 10', 'Windows 11', 'macOS 14', 'macOS 13', 'macOS 12', 'iOS 17', 'iOS 16', 'iOS 15', 'Android 14', 'Android 13', 'Android 12']
        device_types = ['Desktop', 'Mobile', 'Tablet']
        screen_resolutions = ['1920x1080', '1366x768', '1440x900', '2560x1440', '3840x2160', '375x667', '414x896', '390x844', '428x926', '1024x768', '1366x1024', '2048x2732']
        traffic_sources = ['direct', 'google', 'facebook', 'instagram', 'email', 'referral', 'paid_search', 'youtube', 'tiktok', 'pinterest', 'influencer', 'affiliate']
        mediums = ['organic', 'cpc', 'email', 'social', 'referral', 'direct', 'display', 'video', 'influencer']
        
        visitor_id = str(uuid.uuid4())
        customer_id = str(uuid.uuid4())
        customer_segment = random.choice(customer_segments)
        sport_preference = random.choice(sport_preferences)
        size_preference = random.choice(size_preferences)
        clv_tiers = ['low', 'medium', 'high', 'premium']
        customer_lifetime_value_tier = random.choice(clv_tiers)
        loyalty_member = random.choice(['yes', 'no', 'pending'])
        
        state = fake.state()
        city = fake.city()
        zip_code = fake.zipcode()
        ip_address = fake.ipv4()
        
        journey_name = random.choice(list(journey_templates.keys()))
        journey_template = journey_templates[journey_name]
        
        event_sequence = []
        for event_category, count in journey_template['base_flow']:
            selected_events = random.sample(shared_events[event_category], min(count, len(shared_events[event_category])))
            event_sequence.extend(selected_events)
        
        if random.random() < 0.25:
            extra_categories = [cat for cat in shared_events.keys() if cat not in ['exits']]
            extra_category = random.choice(extra_categories)
            extra_event = random.choice(shared_events[extra_category])
            insert_pos = random.randint(1, len(event_sequence) - 1)
            event_sequence.insert(insert_pos, extra_event)
        
        device = random.choice(device_types)
        is_mobile = device in ['Mobile', 'Tablet']
        
        if device == 'Mobile':
            browser = random.choice(['Mobile Safari', 'Chrome Mobile', 'Samsung Internet'])
            if browser == 'Mobile Safari':
                os = random.choice(['iOS 17', 'iOS 16', 'iOS 15'])
            else:
                os = random.choice(['Android 14', 'Android 13', 'Android 12'])
        elif device == 'Tablet':
            browser = random.choice(['Safari', 'Chrome', 'Mobile Safari'])
            os = random.choice(['iOS 17', 'iOS 16', 'macOS 14']) if 'Safari' in browser else random.choice(['Android 14', 'Windows 11'])
        else:
            browser = random.choice(['Chrome', 'Safari', 'Firefox', 'Edge'])
            if browser == 'Safari':
                os = random.choice(['macOS 14', 'macOS 13', 'macOS 12'])
            else:
                os = random.choice(['Windows 11', 'Windows 10', 'macOS 14'])
        
        browser_version = random.choice(browser_versions)
        resolution = random.choice(screen_resolutions)
        user_agent = f"{browser}/{browser_version} ({os})"
        
        is_mobile_app = is_mobile and random.random() < 0.3
        channel = 'mobile_app' if is_mobile_app and device == 'Mobile' else f"web_{device.lower()}"
        
        has_campaign = random.random() < 0.40
        campaign_id = str(uuid.uuid4()) if has_campaign else None
        traffic_source = random.choice(traffic_sources) if has_campaign else 'direct'
        medium = random.choice(mediums) if has_campaign else 'direct'
        referrer_domain = fake.domain_name() if traffic_source == 'referral' else None
        
        journey_start = datetime.now() - timedelta(days=random.randint(0, 90), hours=random.randint(6, 23), minutes=random.randint(0, 59))
        
        site_section_mapping = {
            'marketing': 'Marketing & Promotions', 'authentication': 'Account & Login', 'product_listing': 'Product Catalog', 'product_detail': 'Product Details',
            'ecommerce': 'Shopping & Checkout', 'account': 'My Account', 'wishlist': 'Wishlist & Favorites', 'social': 'Community & Reviews',
            'support': 'Customer Support', 'mobile': 'Mobile Experience', 'tools': 'Shopping Tools', 'search': 'Search & Discovery'
        }
        
        previous_url = None
        converted = False
        
        for i, event_type in enumerate(event_sequence):
            if i == 0:
                event_timestamp = journey_start
            else:
                prev_category = event_details.get(event_sequence[i-1], {}).get('category', '')
                curr_category = event_details.get(event_type, {}).get('category', '')
                if prev_category == curr_category:
                    gap_seconds = random.randint(10, 90)
                elif curr_category == 'checkout':
                    gap_seconds = random.randint(30, 180)
                else:
                    gap_seconds = random.randint(30, 300)
                event_timestamp = previous_timestamp + timedelta(seconds=gap_seconds)
            
            previous_timestamp = event_timestamp
            
            event_info = event_details.get(event_type, {'name': event_type.replace('_', ' ').title(), 'url': f'/{event_type.replace("_", "-")}', 'type': 'general', 'category': 'other'})
            
            product_name = random.choice(product_names) if event_info['category'] in ['product', 'cart', 'checkout', 'purchase'] else None
            product_category = random.choice(product_categories) if product_name else None
            product_brand = random.choice(product_brands) if product_name else None
            product_price = round(random.uniform(15, 300), 2) if product_name else None
            quantity = random.randint(1, 3) if product_name else None
            discount_amount = round(product_price * random.uniform(0, 0.3), 2) if product_price and random.random() < 0.3 else None
            
            payment_method = random.choice(payment_methods) if event_type == 'payment_method_selection' else None
            shipping_method = random.choice(shipping_methods) if event_type == 'shipping_info_entry' else None
            
            order_value = None
            revenue_impact = None
            
            conversion_events = ['purchase_completion', 'order_confirmation_view']
            is_conversion_event = event_type in conversion_events
            
            if is_conversion_event and random.random() < journey_template['conversion_rate']:
                converted = True
                if journey_template['revenue_range'][1] > 0:
                    order_value = round(random.uniform(*journey_template['revenue_range']), 2)
                    revenue_impact = order_value
            
            product_views = 1 if event_info['category'] in ['product', 'browse'] else 0
            add_to_cart_events = 1 if event_type == 'add_to_cart' else 0
            purchase_events = 1 if event_type == 'purchase_completion' else 0
            search_events = 1 if event_info['category'] == 'search' else 0
            wishlist_additions = 1 if event_type == 'add_to_wishlist' else 0
            
            time_on_page = random.randint(5, 600)
            scroll_depth = random.randint(10, 100)
            clicks_on_page = random.randint(0, 25)
            page_load_time = random.randint(100, 4000)
            
            yield (str(uuid.uuid4()), visitor_id, customer_id, event_timestamp, event_type, event_info['category'], event_type.replace('_', ' ').title(), 
                   f"{journey_template['primary_goal']}_{event_type}", event_info['name'], event_info['url'], event_info['type'], 
                   site_section_mapping.get(event_info['type'], 'Other'), previous_url, browser, browser_version, os, device, resolution, user_agent, 
                   ip_address, 'United States', state, city, zip_code, time_on_page, scroll_depth, clicks_on_page, product_name, product_category, 
                   product_brand, product_price, order_value, quantity, discount_amount, payment_method, shipping_method, campaign_id, traffic_source, 
                   medium, referrer_domain, customer_segment, customer_lifetime_value_tier, sport_preference, size_preference, loyalty_member, 
                   product_views, add_to_cart_events, purchase_events, search_events, wishlist_additions, is_mobile_app, page_load_time, 
                   is_conversion_event and converted, revenue_impact)
            
            previous_url = event_info['url']
$$;

-- ===========================================================================
-- SECTION 4: GENERATE RETAIL DATA
-- ===========================================================================

CREATE OR REPLACE TABLE retail_events AS
SELECT e.*
FROM TABLE(GENERATOR(ROWCOUNT => $JOURNEY_COUNT)) g
CROSS JOIN TABLE(generate_ecommerce_journey()) e;

ALTER TABLE retail_events CLUSTER BY (customer_id, event_timestamp);

GRANT SELECT ON ALL TABLES IN SCHEMA SEQUENT_DB.RETAIL TO ROLE SEQUENT_ROLE;


-- ===========================================================================
-- SECTION 5: GENERATE FSI DATA
-- ===========================================================================

USE SCHEMA SEQUENT_DB.FSI;

CREATE OR REPLACE FUNCTION generate_fsi_user_journey()
RETURNS TABLE (
    -- Core identifiers
    event_id STRING,
    visitor_id STRING,
    customer_id STRING,
    
    -- Event details
    event_timestamp TIMESTAMP,
    event_type STRING,
    event_category STRING,
    event_action STRING,
    event_label STRING,
    
    -- Page/Screen information (Adobe Analytics style)
    page_name STRING,
    page_url STRING,
    page_type STRING,
    site_section STRING,
    referrer_url STRING,
    
    -- Technical details
    browser STRING,
    browser_version STRING,
    operating_system STRING,
    device_type STRING,
    screen_resolution STRING,
    user_agent STRING,
    ip_address STRING,
    
    -- Geographic data
    country STRING,
    state STRING,
    city STRING,
    zip_code STRING,
    
    -- Page interaction details
    time_on_page INT,
    scroll_depth INT,
    clicks_on_page INT,
    
    -- Banking specific fields
    account_type STRING,
    product_category STRING,
    transaction_amount DECIMAL(12,2),
    channel STRING,
    authentication_method STRING,
    customer_segment STRING,
    
    -- Campaign/Marketing (Adobe Analytics style)
    campaign_id STRING,
    traffic_source STRING,
    medium STRING,
    referrer_domain STRING,
    
    -- Custom dimensions with explicit names
    customer_tenure STRING,
    account_balance_tier STRING,
    product_interest STRING,
    mobile_app_version STRING,
    customer_lifetime_value_tier STRING,
    
    -- Custom events with explicit names
    form_starts INT,
    form_completions INT,
    errors_encountered INT,
    support_interactions INT,
    product_views INT,
    
    -- Additional context
    is_mobile_app BOOLEAN,
    page_load_time_ms INT,
    conversion_flag BOOLEAN,
    revenue_impact DECIMAL(12,2)
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'generateJourney'
PACKAGES = ('faker')
AS $$
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

class generateJourney:
    def process(self):
        # Define shared event pools that can be used across multiple journeys
        shared_events = {
            'entry_points': [
                'homepage_visit', 'direct_login', 'email_campaign_click', 'search_result_click',
                'social_media_click', 'mobile_app_open', 'branch_referral_visit'
            ],
            'authentication': [
                'login_attempt', 'login_success', 'password_reset_request', 'two_factor_challenge',
                'biometric_auth', 'security_question_prompt', 'account_locked_warning'
            ],
            'account_management': [
                'account_summary_view', 'balance_check', 'transaction_history_view',
                'statement_download', 'account_settings_view', 'profile_update_start',
                'contact_info_update', 'notification_preferences'
            ],
            'product_research': [
                'product_page_visit', 'product_comparison_tool', 'rate_lookup',
                'calculator_use', 'feature_comparison', 'eligibility_checker',
                'testimonial_view', 'faq_browse', 'terms_conditions_view'
            ],
            'application_process': [
                'application_landing', 'application_start', 'personal_info_entry',
                'financial_info_entry', 'document_upload', 'identity_verification',
                'application_review', 'application_submit', 'application_confirmation'
            ],
            'transactional': [
                'transfer_initiate', 'transfer_setup', 'payment_scheduling',
                'payee_management', 'payment_confirmation', 'transaction_receipt_view',
                'recurring_payment_setup', 'payment_history_view'
            ],
            'investment_activities': [
                'portfolio_overview', 'market_dashboard', 'stock_research',
                'trade_preparation', 'order_entry', 'trade_execution',
                'performance_review', 'rebalancing_tools'
            ],
            'support_touchpoints': [
                'help_center_visit', 'search_help_articles', 'faq_section_browse',
                'contact_options_view', 'live_chat_initiate', 'phone_callback_request',
                'support_ticket_creation', 'branch_appointment_booking',
                'agent_interaction', 'issue_escalation', 'resolution_confirmation',
                'feedback_survey'
            ],
            'mobile_specific': [
                'mobile_dashboard', 'quick_balance_check', 'mobile_deposit_camera',
                'location_services_enable', 'push_notification_interaction',
                'app_settings_access', 'biometric_setup'
            ],
            'security_actions': [
                'security_center_visit', 'fraud_alert_review', 'card_management',
                'travel_notification_setup', 'security_settings_update',
                'device_management', 'suspicious_activity_review'
            ],
            'cross_selling': [
                'product_recommendation_view', 'promotional_banner_click',
                'upgrade_offer_consideration', 'additional_product_research',
                'cross_sell_application_start'
            ],
            'exits': [
                'logout', 'session_timeout', 'navigation_away', 'app_background',
                'browser_close', 'phone_call_transfer'
            ]
        }
        
        # Define journey templates with shared events and branching logic
        journey_templates = {
            'new_customer_exploration': {
                'primary_goal': 'account_opening',
                'base_flow': [
                    ('entry_points', 1),
                    ('product_research', random.randint(2, 4)),
                    ('support_touchpoints', random.randint(0, 2)),  # May need help
                    ('application_process', random.randint(3, 7)),
                    ('account_management', random.randint(1, 2)),  # Check new account
                    ('exits', 1)
                ],
                'conversion_rate': 0.6,
                'revenue_range': (100, 500)
            },
            'existing_customer_expansion': {
                'primary_goal': 'product_addition',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', random.randint(1, 2)),
                    ('account_management', random.randint(2, 3)),
                    ('cross_selling', random.randint(1, 2)),
                    ('product_research', random.randint(1, 3)),
                    ('application_process', random.randint(2, 5)),
                    ('transactional', random.randint(0, 2)),  # May do other banking
                    ('exits', 1)
                ],
                'conversion_rate': 0.45,
                'revenue_range': (200, 2000)
            },
            'loan_shopping_journey': {
                'primary_goal': 'loan_application',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', random.randint(0, 2)),  # May browse without login
                    ('product_research', random.randint(3, 5)),
                    ('support_touchpoints', random.randint(1, 3)),  # Likely need guidance
                    ('account_management', random.randint(0, 2)),  # Check existing accounts
                    ('application_process', random.randint(2, 6)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.35,
                'revenue_range': (1000, 10000)
            },
            'routine_banking_session': {
                'primary_goal': 'transaction_completion',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', random.randint(1, 2)),
                    ('account_management', random.randint(2, 4)),
                    ('transactional', random.randint(2, 4)),
                    ('cross_selling', random.randint(0, 1)),  # May see offers
                    ('product_research', random.randint(0, 2)),  # May browse
                    ('exits', 1)
                ],
                'conversion_rate': 0.85,
                'revenue_range': (0, 0)  # No direct revenue
            },
            'investment_management': {
                'primary_goal': 'trade_execution',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', random.randint(1, 2)),
                    ('investment_activities', random.randint(3, 6)),
                    ('account_management', random.randint(0, 2)),
                    ('support_touchpoints', random.randint(0, 1)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.55,
                'revenue_range': (5, 50)
            },
            'support_resolution': {
                'primary_goal': 'issue_resolution',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', random.randint(0, 2)),
                    ('support_touchpoints', random.randint(3, 6)),
                    ('account_management', random.randint(1, 3)),  # Review account details
                    ('security_actions', random.randint(0, 2)),  # May involve security
                    ('transactional', random.randint(0, 1)),  # May do transactions
                    ('exits', 1)
                ],
                'conversion_rate': 0.75,
                'revenue_range': (0, 0)
            },
            'mobile_banking_session': {
                'primary_goal': 'mobile_transaction',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', 1),
                    ('mobile_specific', random.randint(2, 4)),
                    ('account_management', random.randint(1, 3)),
                    ('transactional', random.randint(1, 3)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.80,
                'revenue_range': (0, 0)
            },
            'account_closure_journey': {
                'primary_goal': 'account_closure',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', random.randint(1, 2)),
                    ('account_management', random.randint(2, 3)),  # Review accounts
                    ('support_touchpoints', random.randint(2, 4)),  # Need help closing
                    ('transactional', random.randint(1, 3)),  # Transfer remaining funds
                    ('security_actions', random.randint(0, 2)),  # Security verification
                    ('support_touchpoints', random.randint(1, 2)),  # Final confirmation
                    ('exits', 1)
                ],
                'conversion_rate': 0.40,  # May retain customer
                'revenue_range': (-500, 0)  # Negative revenue impact
            },
            'security_incident_response': {
                'primary_goal': 'security_resolution',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', random.randint(1, 3)),  # Multiple auth attempts
                    ('security_actions', random.randint(3, 5)),
                    ('support_touchpoints', random.randint(2, 4)),
                    ('account_management', random.randint(1, 2)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.70,
                'revenue_range': (0, 0)
            },
            'research_abandonment': {
                'primary_goal': 'research_only',
                'base_flow': [
                    ('entry_points', 1),
                    ('product_research', random.randint(2, 5)),
                    ('support_touchpoints', random.randint(0, 2)),
                    ('authentication', random.randint(0, 1)),  # May not even log in
                    ('application_process', random.randint(0, 2)),  # Partial application
                    ('exits', 1)
                ],
                'conversion_rate': 0.05,  # Very low conversion
                'revenue_range': (0, 0)
            }
        }
        
        # Detailed event mappings
        event_details = {
            # Entry points
            'homepage_visit': {'name': 'Homepage', 'url': '/', 'type': 'marketing', 'category': 'navigation'},
            'direct_login': {'name': 'Direct Login', 'url': '/login', 'type': 'authentication', 'category': 'authentication'},
            'email_campaign_click': {'name': 'Email Campaign', 'url': '/campaign-landing', 'type': 'marketing', 'category': 'campaign'},
            'search_result_click': {'name': 'Search Results', 'url': '/search-landing', 'type': 'marketing', 'category': 'acquisition'},
            'social_media_click': {'name': 'Social Media', 'url': '/social-landing', 'type': 'marketing', 'category': 'social'},
            'mobile_app_open': {'name': 'Mobile App Home', 'url': '/app', 'type': 'mobile', 'category': 'mobile_banking'},
            'branch_referral_visit': {'name': 'Branch Referral', 'url': '/branch-referral', 'type': 'marketing', 'category': 'branch'},
            
            # Authentication
            'login_attempt': {'name': 'Login Attempt', 'url': '/login', 'type': 'authentication', 'category': 'authentication'},
            'login_success': {'name': 'Login Success', 'url': '/dashboard', 'type': 'authentication', 'category': 'authentication'},
            'password_reset_request': {'name': 'Password Reset', 'url': '/password-reset', 'type': 'authentication', 'category': 'security'},
            'two_factor_challenge': {'name': '2FA Challenge', 'url': '/2fa-verify', 'type': 'authentication', 'category': 'security'},
            'biometric_auth': {'name': 'Biometric Auth', 'url': '/biometric-login', 'type': 'authentication', 'category': 'security'},
            'security_question_prompt': {'name': 'Security Questions', 'url': '/security-questions', 'type': 'authentication', 'category': 'security'},
            'account_locked_warning': {'name': 'Account Locked', 'url': '/account-locked', 'type': 'security', 'category': 'security'},
            
            # Account Management
            'account_summary_view': {'name': 'Account Summary', 'url': '/accounts', 'type': 'account_management', 'category': 'account_management'},
            'balance_check': {'name': 'Balance Check', 'url': '/accounts/balance', 'type': 'account_management', 'category': 'account_management'},
            'transaction_history_view': {'name': 'Transaction History', 'url': '/accounts/transactions', 'type': 'account_management', 'category': 'account_management'},
            'statement_download': {'name': 'Download Statement', 'url': '/accounts/statements', 'type': 'account_management', 'category': 'account_management'},
            'account_settings_view': {'name': 'Account Settings', 'url': '/settings/account', 'type': 'account_management', 'category': 'account_management'},
            'profile_update_start': {'name': 'Update Profile', 'url': '/profile/edit', 'type': 'account_management', 'category': 'account_management'},
            'contact_info_update': {'name': 'Update Contact Info', 'url': '/profile/contact', 'type': 'account_management', 'category': 'account_management'},
            'notification_preferences': {'name': 'Notification Settings', 'url': '/settings/notifications', 'type': 'account_management', 'category': 'account_management'},
            
            # Product Research
            'product_page_visit': {'name': 'Products Overview', 'url': '/products', 'type': 'marketing', 'category': 'product_research'},
            'product_comparison_tool': {'name': 'Product Comparison', 'url': '/products/compare', 'type': 'tools', 'category': 'product_research'},
            'rate_lookup': {'name': 'Interest Rates', 'url': '/rates', 'type': 'marketing', 'category': 'product_research'},
            'calculator_use': {'name': 'Financial Calculator', 'url': '/tools/calculator', 'type': 'tools', 'category': 'product_research'},
            'feature_comparison': {'name': 'Feature Comparison', 'url': '/products/features', 'type': 'marketing', 'category': 'product_research'},
            'eligibility_checker': {'name': 'Eligibility Check', 'url': '/tools/eligibility', 'type': 'tools', 'category': 'product_research'},
            'testimonial_view': {'name': 'Customer Testimonials', 'url': '/testimonials', 'type': 'marketing', 'category': 'product_research'},
            'faq_browse': {'name': 'FAQ Browse', 'url': '/faq', 'type': 'support', 'category': 'product_research'},
            'terms_conditions_view': {'name': 'Terms & Conditions', 'url': '/legal/terms', 'type': 'legal', 'category': 'product_research'},
            
            # Application Process
            'application_landing': {'name': 'Application Landing', 'url': '/apply', 'type': 'application', 'category': 'application_process'},
            'application_start': {'name': 'Start Application', 'url': '/apply/start', 'type': 'application', 'category': 'application_process'},
            'personal_info_entry': {'name': 'Personal Information', 'url': '/apply/personal', 'type': 'application', 'category': 'application_process'},
            'financial_info_entry': {'name': 'Financial Information', 'url': '/apply/financial', 'type': 'application', 'category': 'application_process'},
            'document_upload': {'name': 'Document Upload', 'url': '/apply/documents', 'type': 'application', 'category': 'application_process'},
            'identity_verification': {'name': 'Identity Verification', 'url': '/apply/verify', 'type': 'application', 'category': 'application_process'},
            'application_review': {'name': 'Review Application', 'url': '/apply/review', 'type': 'application', 'category': 'application_process'},
            'application_submit': {'name': 'Submit Application', 'url': '/apply/submit', 'type': 'application', 'category': 'application_process'},
            'application_confirmation': {'name': 'Application Confirmation', 'url': '/apply/confirmation', 'type': 'application', 'category': 'application_process'},
            
            # Transactional
            'transfer_initiate': {'name': 'Initiate Transfer', 'url': '/transfer', 'type': 'transaction', 'category': 'transactional'},
            'transfer_setup': {'name': 'Transfer Setup', 'url': '/transfer/setup', 'type': 'transaction', 'category': 'transactional'},
            'payment_scheduling': {'name': 'Schedule Payment', 'url': '/payments/schedule', 'type': 'transaction', 'category': 'transactional'},
            'payee_management': {'name': 'Manage Payees', 'url': '/payments/payees', 'type': 'transaction', 'category': 'transactional'},
            'payment_confirmation': {'name': 'Payment Confirmation', 'url': '/payments/confirm', 'type': 'transaction', 'category': 'transactional'},
            'transaction_receipt_view': {'name': 'Transaction Receipt', 'url': '/receipts', 'type': 'transaction', 'category': 'transactional'},
            'recurring_payment_setup': {'name': 'Recurring Payments', 'url': '/payments/recurring', 'type': 'transaction', 'category': 'transactional'},
            'payment_history_view': {'name': 'Payment History', 'url': '/payments/history', 'type': 'transaction', 'category': 'transactional'},
            
            # Investment Activities
            'portfolio_overview': {'name': 'Portfolio Overview', 'url': '/investments', 'type': 'investment', 'category': 'investment_activities'},
            'market_dashboard': {'name': 'Market Dashboard', 'url': '/investments/market', 'type': 'investment', 'category': 'investment_activities'},
            'stock_research': {'name': 'Stock Research', 'url': '/investments/research', 'type': 'investment', 'category': 'investment_activities'},
            'trade_preparation': {'name': 'Trade Preparation', 'url': '/investments/trade-prep', 'type': 'investment', 'category': 'investment_activities'},
            'order_entry': {'name': 'Order Entry', 'url': '/investments/order', 'type': 'investment', 'category': 'investment_activities'},
            'trade_execution': {'name': 'Trade Execution', 'url': '/investments/execute', 'type': 'investment', 'category': 'investment_activities'},
            'performance_review': {'name': 'Performance Review', 'url': '/investments/performance', 'type': 'investment', 'category': 'investment_activities'},
            'rebalancing_tools': {'name': 'Portfolio Rebalancing', 'url': '/investments/rebalance', 'type': 'investment', 'category': 'investment_activities'},
            
            # Support Touchpoints
            'help_center_visit': {'name': 'Help Center', 'url': '/help', 'type': 'support', 'category': 'support_touchpoints'},
            'search_help_articles': {'name': 'Search Help', 'url': '/help/search', 'type': 'support', 'category': 'support_touchpoints'},
            'faq_section_browse': {'name': 'FAQ Section', 'url': '/help/faq', 'type': 'support', 'category': 'support_touchpoints'},
            'contact_options_view': {'name': 'Contact Options', 'url': '/contact', 'type': 'support', 'category': 'support_touchpoints'},
            'live_chat_initiate': {'name': 'Start Live Chat', 'url': '/support/chat', 'type': 'support', 'category': 'support_touchpoints'},
            'phone_callback_request': {'name': 'Request Callback', 'url': '/support/callback', 'type': 'support', 'category': 'support_touchpoints'},
            'support_ticket_creation': {'name': 'Create Support Ticket', 'url': '/support/ticket', 'type': 'support', 'category': 'support_touchpoints'},
            'branch_appointment_booking': {'name': 'Book Branch Appointment', 'url': '/branch/appointment', 'type': 'support', 'category': 'support_touchpoints'},
            'agent_interaction': {'name': 'Agent Interaction', 'url': '/support/agent', 'type': 'support', 'category': 'support_touchpoints'},
            'issue_escalation': {'name': 'Issue Escalation', 'url': '/support/escalate', 'type': 'support', 'category': 'support_touchpoints'},
            'resolution_confirmation': {'name': 'Issue Resolved', 'url': '/support/resolved', 'type': 'support', 'category': 'support_touchpoints'},
            'feedback_survey': {'name': 'Feedback Survey', 'url': '/support/feedback', 'type': 'support', 'category': 'support_touchpoints'},
            
            # Mobile Specific
            'mobile_dashboard': {'name': 'Mobile Dashboard', 'url': '/mobile/dashboard', 'type': 'mobile', 'category': 'mobile_specific'},
            'quick_balance_check': {'name': 'Quick Balance', 'url': '/mobile/balance', 'type': 'mobile', 'category': 'mobile_specific'},
            'mobile_deposit_camera': {'name': 'Mobile Deposit', 'url': '/mobile/deposit', 'type': 'mobile', 'category': 'mobile_specific'},
            'location_services_enable': {'name': 'Enable Location', 'url': '/mobile/location', 'type': 'mobile', 'category': 'mobile_specific'},
            'push_notification_interaction': {'name': 'Push Notification', 'url': '/mobile/notifications', 'type': 'mobile', 'category': 'mobile_specific'},
            'app_settings_access': {'name': 'App Settings', 'url': '/mobile/settings', 'type': 'mobile', 'category': 'mobile_specific'},
            'biometric_setup': {'name': 'Biometric Setup', 'url': '/mobile/biometric', 'type': 'mobile', 'category': 'mobile_specific'},
            
            # Security Actions
            'security_center_visit': {'name': 'Security Center', 'url': '/security', 'type': 'security', 'category': 'security_actions'},
            'fraud_alert_review': {'name': 'Fraud Alerts', 'url': '/security/fraud', 'type': 'security', 'category': 'security_actions'},
            'card_management': {'name': 'Card Management', 'url': '/security/cards', 'type': 'security', 'category': 'security_actions'},
            'travel_notification_setup': {'name': 'Travel Notification', 'url': '/security/travel', 'type': 'security', 'category': 'security_actions'},
            'security_settings_update': {'name': 'Security Settings', 'url': '/security/settings', 'type': 'security', 'category': 'security_actions'},
            'device_management': {'name': 'Device Management', 'url': '/security/devices', 'type': 'security', 'category': 'security_actions'},
            'suspicious_activity_review': {'name': 'Suspicious Activity', 'url': '/security/suspicious', 'type': 'security', 'category': 'security_actions'},
            
            # Cross-selling
            'product_recommendation_view': {'name': 'Product Recommendations', 'url': '/recommendations', 'type': 'marketing', 'category': 'cross_selling'},
            'promotional_banner_click': {'name': 'Promotional Banner', 'url': '/promotions', 'type': 'marketing', 'category': 'cross_selling'},
            'upgrade_offer_consideration': {'name': 'Upgrade Offer', 'url': '/upgrade', 'type': 'marketing', 'category': 'cross_selling'},
            'additional_product_research': {'name': 'Additional Products', 'url': '/products/additional', 'type': 'marketing', 'category': 'cross_selling'},
            'cross_sell_application_start': {'name': 'Cross-sell Application', 'url': '/apply/cross-sell', 'type': 'application', 'category': 'cross_selling'},
            
            # Exits
            'logout': {'name': 'Logout', 'url': '/logout', 'type': 'authentication', 'category': 'exits'},
            'session_timeout': {'name': 'Session Timeout', 'url': '/timeout', 'type': 'system', 'category': 'exits'},
            'navigation_away': {'name': 'Navigate Away', 'url': '/external', 'type': 'system', 'category': 'exits'},
            'app_background': {'name': 'App Background', 'url': '/mobile/background', 'type': 'mobile', 'category': 'exits'},
            'browser_close': {'name': 'Browser Close', 'url': '/close', 'type': 'system', 'category': 'exits'},
            'phone_call_transfer': {'name': 'Phone Transfer', 'url': '/phone-transfer', 'type': 'support', 'category': 'exits'}
        }
        
        # Banking products and categories
        account_types = [
            'checking', 'savings', 'money_market', 'cd', 'credit_card', 
            'mortgage', 'auto_loan', 'personal_loan', 'heloc', 'investment_account',
            'business_checking', 'business_savings', 'business_loan'
        ]
        
        product_categories = [
            'deposit_accounts', 'credit_products', 'lending', 'investment_services',
            'insurance', 'wealth_management', 'business_banking', 'digital_services'
        ]
        
        customer_segments = [
            'mass_market', 'emerging_affluent', 'affluent', 'high_net_worth', 
            'ultra_high_net_worth', 'small_business', 'commercial', 'student',
            'senior', 'military'
        ]
        
        channels = ['web_desktop', 'web_mobile', 'mobile_app', 'tablet_app']
        
        # Technical configurations
        browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Mobile Safari', 'Chrome Mobile', 'Samsung Internet']
        browser_versions = ['120.0', '119.0', '118.0', '117.0', '116.0', '115.0']
        operating_systems = [
            'Windows 10', 'Windows 11', 'macOS 14', 'macOS 13', 'macOS 12',
            'iOS 17', 'iOS 16', 'iOS 15', 'Android 14', 'Android 13', 'Android 12'
        ]
        device_types = ['Desktop', 'Mobile', 'Tablet']
        screen_resolutions = [
            '1920x1080', '1366x768', '1440x900', '2560x1440', '3840x2160',
            '375x667', '414x896', '390x844', '428x926',  # iPhone
            '1024x768', '1366x1024', '2048x2732'  # iPad
        ]
        
        # Campaign sources
        traffic_sources = [
            'direct', 'google', 'facebook', 'email', 'referral', 'paid_search',
            'youtube', 'linkedin', 'twitter', 'instagram'
        ]
        mediums = ['organic', 'cpc', 'email', 'social', 'referral', 'direct', 'display', 'video', 'affiliate']
        
        # Authentication methods
        auth_methods = [
            'username_password', 'biometric_fingerprint', 'biometric_face_id', 
            'sms_otp', 'email_otp', 'hardware_token', 'push_notification',
            'security_questions', 'voice_recognition'
        ]
        
        # Generate consistent user profile for this journey
        visitor_id = str(uuid.uuid4())
        customer_id = str(uuid.uuid4())
        customer_segment = random.choice(customer_segments)
        tenure_months = random.randint(1, 240)
        balance_tier = random.choices(
            ['low', 'medium', 'high', 'premium', 'private'],
            weights=[40, 30, 20, 8, 2]
        )[0]
        
        # Consistent geographic data
        state = fake.state()
        city = fake.city()
        zip_code = fake.zipcode()
        ip_address = fake.ipv4()  # Same IP for the journey
        
        # Choose a journey template
        journey_name = random.choice(list(journey_templates.keys()))
        journey_template = journey_templates[journey_name]
        
        # Build the actual event sequence from the template
        event_sequence = []
        for event_category, count in journey_template['base_flow']:
            selected_events = random.sample(shared_events[event_category], min(count, len(shared_events[event_category])))
            event_sequence.extend(selected_events)
        
        # Add some randomization - 20% chance to add extra cross-category events
        if random.random() < 0.2:
            extra_categories = [cat for cat in shared_events.keys() if cat not in ['exits']]
            extra_category = random.choice(extra_categories)
            extra_event = random.choice(shared_events[extra_category])
            # Insert at random position (not at the end)
            insert_pos = random.randint(1, len(event_sequence) - 1)
            event_sequence.insert(insert_pos, extra_event)
        
        # Consistent technical details for the journey
        device = random.choice(device_types)
        is_mobile = device in ['Mobile', 'Tablet']
        
        # Choose browser based on device
        if device == 'Mobile':
            browser = random.choice(['Mobile Safari', 'Chrome Mobile', 'Samsung Internet'])
            if browser == 'Mobile Safari':
                os = random.choice(['iOS 17', 'iOS 16', 'iOS 15'])
            else:
                os = random.choice(['Android 14', 'Android 13', 'Android 12'])
        elif device == 'Tablet':
            browser = random.choice(['Safari', 'Chrome', 'Mobile Safari'])
            os = random.choice(['iOS 17', 'iOS 16', 'macOS 14']) if 'Safari' in browser else random.choice(['Android 14', 'Windows 11'])
        else:  # Desktop
            browser = random.choice(['Chrome', 'Safari', 'Firefox', 'Edge'])
            if browser == 'Safari':
                os = random.choice(['macOS 14', 'macOS 13', 'macOS 12'])
            else:
                os = random.choice(['Windows 11', 'Windows 10', 'macOS 14'])
        
        browser_version = random.choice(browser_versions)
        resolution = random.choice(screen_resolutions)
        user_agent = f"{browser}/{browser_version} ({os})"
        
        # Channel determination
        is_mobile_app = is_mobile and random.random() < 0.4
        if is_mobile_app:
            channel = 'mobile_app' if device == 'Mobile' else 'tablet_app'
        else:
            channel = f"web_{device.lower()}"
        
        # Campaign attribution (consistent for the journey)
        has_campaign = random.random() < 0.35
        campaign_id = str(uuid.uuid4()) if has_campaign else None
        traffic_source = random.choice(traffic_sources) if has_campaign else 'direct'
        medium = random.choice(mediums) if has_campaign else 'direct'
        referrer_domain = fake.domain_name() if traffic_source == 'referral' else None
        
        # Authentication method (consistent for journey)
        auth_method = random.choice(auth_methods)
        
        # Account type and product category
        account_type = random.choice(account_types)
        product_category = random.choice(product_categories)
        
        # Custom dimensions with explicit names (consistent for journey)
        customer_tenure = f"{tenure_months}_months"
        account_balance_tier = balance_tier
        product_interest = journey_template['primary_goal']
        mobile_app_version = f"v{random.randint(1, 15)}.{random.randint(0, 9)}.{random.randint(0, 9)}" if is_mobile_app else None
        
        clv_tiers = ['low', 'medium', 'high', 'premium']
        clv_weights = [40, 35, 20, 5] if balance_tier == 'low' else [10, 30, 40, 20]
        customer_lifetime_value_tier = random.choices(clv_tiers, weights=clv_weights)[0]
        
        # Generate journey start time
        journey_start = datetime.now() - timedelta(
            days=random.randint(0, 90),
            hours=random.randint(6, 23),
            minutes=random.randint(0, 59)
        )
        
        # Site section mapping
        site_section_mapping = {
            'authentication': 'Login & Security',
            'account_management': 'My Accounts',
            'marketing': 'Products & Services',
            'tools': 'Financial Tools',
            'application': 'Applications',
            'support': 'Customer Service',
            'transaction': 'Banking Services',
            'investment': 'Investments',
            'security': 'Security & Settings',
            'mobile': 'Mobile Banking',
            'legal': 'Legal & Compliance',
            'system': 'System'
        }
        
        # Generate events for the journey
        previous_url = None
        converted = False
        
        for i, event_type in enumerate(event_sequence):
            # Calculate event timestamp with more realistic gaps
            if i == 0:
                event_timestamp = journey_start
            else:
                # Variable time gaps - shorter for related actions, longer for different categories
                prev_category = event_details.get(event_sequence[i-1], {}).get('category', '')
                curr_category = event_details.get(event_type, {}).get('category', '')
                
                if prev_category == curr_category:
                    gap_seconds = random.randint(15, 120)  # 15 seconds to 2 minutes for related actions
                else:
                    gap_seconds = random.randint(60, 600)  # 1 to 10 minutes for category changes
                
                event_timestamp = previous_timestamp + timedelta(seconds=gap_seconds)
            
            previous_timestamp = event_timestamp
            
            # Get event information
            event_info = event_details.get(event_type, {
                'name': event_type.replace('_', ' ').title(),
                'url': f'/{event_type.replace("_", "-")}',
                'type': 'general',
                'category': 'other'
            })
            
            # Transaction amounts for specific events
            transaction_amount = None
            revenue_impact = None
            
            # Determine if this is a conversion event
            conversion_events = [
                'application_confirmation', 'trade_execution', 'payment_confirmation',
                'resolution_confirmation', 'account_closed_confirmation'
            ]
            
            is_conversion_event = event_type in conversion_events
            
            if is_conversion_event and random.random() < journey_template['conversion_rate']:
                converted = True
                if journey_template['revenue_range'][1] > 0:
                    revenue_impact = round(random.uniform(*journey_template['revenue_range']), 2)
                else:
                    revenue_impact = journey_template['revenue_range'][0]  # Negative or zero
            
            # Transaction amounts based on event type
            if 'payment' in event_type or 'transfer' in event_type:
                transaction_amount = round(random.uniform(25, 5000), 2)
            elif 'trade' in event_type:
                transaction_amount = round(random.uniform(500, 100000), 2)
            
            # Custom events with explicit names (counts)
            form_starts = 1 if any(x in event_type for x in ['start', 'initiate', 'begin']) else 0
            form_completions = 1 if any(x in event_type for x in ['confirmation', 'submit', 'complete', 'success', 'execution']) else 0
            errors_encountered = 1 if random.random() < 0.03 else 0  # 3% error rate
            support_interactions = 1 if event_info['category'] == 'support_touchpoints' else 0
            product_views = 1 if event_info['category'] in ['product_research', 'cross_selling'] else 0
            
            # Page interaction metrics
            time_on_page = random.randint(10, 900)  # 10 seconds to 15 minutes
            scroll_depth = random.randint(5, 100)  # Percentage
            clicks_on_page = random.randint(0, 20)
            page_load_time = random.randint(150, 5000)  # milliseconds
            
            yield (
                # Core identifiers
                str(uuid.uuid4()),  # event_id
                visitor_id,  # visitor_id (consistent)
                customer_id,  # customer_id (consistent)
                
                # Event details
                event_timestamp,  # event_timestamp
                event_type,  # event_type
                event_info['category'],  # event_category
                event_type.replace('_', ' ').title(),  # event_action
                f"{journey_template['primary_goal']}_{event_type}",  # event_label
                
                # Page information
                event_info['name'],  # page_name
                event_info['url'],  # page_url
                event_info['type'],  # page_type
                site_section_mapping.get(event_info['type'], 'Other'),  # site_section
                previous_url,  # referrer_url
                
                # Technical details (consistent for journey)
                browser,  # browser
                browser_version,  # browser_version
                os,  # operating_system
                device,  # device_type
                resolution,  # screen_resolution
                user_agent,  # user_agent
                ip_address,  # ip_address
                
                # Geographic data (consistent for journey)
                'United States',  # country
                state,  # state
                city,  # city
                zip_code,  # zip_code
                
                # Page interaction details
                time_on_page,  # time_on_page
                scroll_depth,  # scroll_depth
                clicks_on_page,  # clicks_on_page
                
                # Banking specific fields
                account_type,  # account_type
                product_category,  # product_category
                transaction_amount,  # transaction_amount
                channel,  # channel
                auth_method,  # authentication_method
                customer_segment,  # customer_segment
                
                # Campaign/Marketing (consistent for journey)
                campaign_id,  # campaign_id
                traffic_source,  # traffic_source
                medium,  # medium
                referrer_domain,  # referrer_domain
                
                # Custom dimensions with explicit names (consistent for journey)
                customer_tenure,  # customer_tenure
                account_balance_tier,  # account_balance_tier
                product_interest,  # product_interest
                mobile_app_version,  # mobile_app_version
                customer_lifetime_value_tier,  # customer_lifetime_value_tier
                
                # Custom events with explicit names
                form_starts,  # form_starts
                form_completions,  # form_completions
                errors_encountered,  # errors_encountered
                support_interactions,  # support_interactions
                product_views,  # product_views
                
                # Additional context
                is_mobile_app,  # is_mobile_app
                page_load_time,  # page_load_time_ms
                is_conversion_event and converted,  # conversion_flag
                revenue_impact  # revenue_impact
            )
            
            # Set previous URL for next iteration
            previous_url = event_info['url']
$$;

CREATE OR REPLACE TABLE fsi_events AS
SELECT e.*
FROM TABLE(GENERATOR(ROWCOUNT => $JOURNEY_COUNT)) g
CROSS JOIN TABLE(generate_fsi_user_journey()) e;

GRANT SELECT ON ALL TABLES IN SCHEMA SEQUENT_DB.FSI TO ROLE SEQUENT_ROLE;

-- ===========================================================================
-- SECTION 6: GENERATE GAMING DATA
-- ===========================================================================

USE SCHEMA SEQUENT_DB.GAMING;

CREATE OR REPLACE FUNCTION generate_gaming_journey()
RETURNS TABLE (
    -- Core identifiers
    event_id STRING,
    user_id STRING,
    player_id STRING,
    session_id STRING,
    
    -- Event details
    event_timestamp TIMESTAMP,
    event_type STRING,
    event_category STRING,
    event_action STRING,
    event_label STRING,
    
    -- Game/Platform information
    game_title STRING,
    game_mode STRING,
    platform STRING,
    game_version STRING,
    level_name STRING,
    
    -- Technical details
    device_type STRING,
    operating_system STRING,
    client_version STRING,
    connection_type STRING,
    fps_average INT,
    ping_ms INT,
    
    -- Geographic data
    country STRING,
    region STRING,
    timezone STRING,
    
    -- Gameplay metrics
    session_duration_minutes INT,
    actions_per_minute INT,
    score_achieved INT,
    
    -- Game-specific fields
    character_class STRING,
    character_level INT,
    current_xp INT,
    currency_earned INT,
    currency_spent INT,
    items_collected STRING,
    achievement_unlocked STRING,
    
    -- Store/Monetization fields
    item_purchased STRING,
    item_category STRING,
    item_rarity STRING,
    purchase_price NUMBER(10,2),
    currency_type STRING,
    total_spent NUMBER(12,2),
    
    -- Player behavior dimensions
    player_segment STRING,
    spending_tier STRING,
    skill_level STRING,
    playtime_category STRING,
    social_activity_level STRING,
    
    -- Custom gaming events
    level_completions INT,
    deaths_count INT,
    kills_count INT,
    items_used INT,
    social_interactions INT,
    
    -- Additional context
    is_premium_player BOOLEAN,
    is_first_session BOOLEAN,
    conversion_flag BOOLEAN,
    revenue_impact NUMBER(12,2)
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'generateJourney'
PACKAGES = ('faker')
AS $$
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

class generateJourney:
    def process(self):
        # Define shared event pools for gaming experiences
        shared_events = {
            'session_start': [
                'game_launch', 'login_success', 'main_menu_enter', 'tutorial_start',
                'continue_game', 'new_game_start', 'profile_load', 'settings_check'
            ],
            'gameplay_core': [
                'level_start', 'level_complete', 'level_failed', 'checkpoint_reached',
                'boss_encounter', 'boss_defeated', 'mission_start', 'mission_complete',
                'quest_accepted', 'quest_completed', 'objective_completed'
            ],
            'character_progression': [
                'level_up', 'xp_gained', 'skill_unlocked', 'ability_upgraded',
                'stat_increased', 'character_created', 'class_selected', 'talent_point_spent',
                'prestige_reached', 'achievement_earned'
            ],
            'combat_events': [
                'enemy_killed', 'player_death', 'damage_dealt', 'damage_taken',
                'critical_hit', 'combo_executed', 'spell_cast', 'item_used_combat',
                'weapon_equipped', 'armor_equipped'
            ],
            'item_collection': [
                'item_found', 'treasure_opened', 'loot_collected', 'rare_drop',
                'crafting_material_found', 'currency_found', 'item_crafted',
                'equipment_upgraded', 'inventory_full', 'item_sold'
            ],
            'social_multiplayer': [
                'friend_added', 'party_joined', 'guild_joined', 'chat_message_sent',
                'voice_chat_started', 'player_invited', 'match_found', 'team_formed',
                'leaderboard_viewed', 'tournament_entered'
            ],
            'store_browsing': [
                'store_opened', 'category_browsed', 'item_previewed', 'item_details_viewed',
                'price_checked', 'bundle_viewed', 'sale_items_browsed', 'wishlist_viewed',
                'recommendations_viewed', 'search_store'
            ],
            'monetization': [
                'item_purchased', 'bundle_purchased', 'currency_purchased', 'premium_upgrade',
                'battle_pass_purchased', 'dlc_purchased', 'cosmetic_purchased',
                'booster_purchased', 'subscription_activated', 'gift_purchased'
            ],
            'customization': [
                'character_customized', 'outfit_changed', 'weapon_skin_applied',
                'base_decorated', 'avatar_updated', 'title_changed', 'emblem_selected',
                'emote_equipped', 'victory_pose_set', 'loadout_saved'
            ],
            'meta_progression': [
                'daily_quest_completed', 'weekly_challenge_finished', 'event_participated',
                'seasonal_reward_claimed', 'battle_pass_tier_unlocked', 'login_bonus_claimed',
                'milestone_reached', 'collection_completed', 'mastery_achieved'
            ],
            'session_end': [
                'game_paused', 'settings_accessed', 'save_game', 'logout',
                'session_timeout', 'connection_lost', 'game_closed', 'platform_exit'
            ]
        }
        
        # Define journey templates for gaming experiences
        journey_templates = {
            'new_player_onboarding': {
                'primary_goal': 'tutorial_completion',
                'base_flow': [
                    ('session_start', 1),
                    ('gameplay_core', random.randint(3, 6)),
                    ('character_progression', random.randint(2, 4)),
                    ('combat_events', random.randint(2, 5)),
                    ('item_collection', random.randint(1, 3)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.65,
                'revenue_range': (0, 10)
            },
            'casual_gaming_session': {
                'primary_goal': 'entertainment',
                'base_flow': [
                    ('session_start', 1),
                    ('gameplay_core', random.randint(2, 4)),
                    ('combat_events', random.randint(1, 3)),
                    ('item_collection', random.randint(1, 2)),
                    ('character_progression', random.randint(0, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.85,
                'revenue_range': (0, 5)
            },
            'hardcore_gaming_session': {
                'primary_goal': 'progression',
                'base_flow': [
                    ('session_start', 1),
                    ('gameplay_core', random.randint(5, 10)),
                    ('combat_events', random.randint(4, 8)),
                    ('character_progression', random.randint(2, 5)),
                    ('item_collection', random.randint(2, 4)),
                    ('meta_progression', random.randint(1, 3)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.90,
                'revenue_range': (0, 50)
            },
            'competitive_multiplayer': {
                'primary_goal': 'ranking_improvement',
                'base_flow': [
                    ('session_start', 1),
                    ('social_multiplayer', random.randint(2, 4)),
                    ('gameplay_core', random.randint(3, 6)),
                    ('combat_events', random.randint(5, 10)),
                    ('character_progression', random.randint(1, 3)),
                    ('meta_progression', random.randint(0, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.75,
                'revenue_range': (0, 25)
            },
            'shopping_spree': {
                'primary_goal': 'store_purchase',
                'base_flow': [
                    ('session_start', 1),
                    ('store_browsing', random.randint(3, 6)),
                    ('monetization', random.randint(1, 4)),
                    ('customization', random.randint(1, 3)),
                    ('gameplay_core', random.randint(0, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.45,
                'revenue_range': (5, 100)
            },
            'social_gaming_session': {
                'primary_goal': 'social_interaction',
                'base_flow': [
                    ('session_start', 1),
                    ('social_multiplayer', random.randint(3, 6)),
                    ('gameplay_core', random.randint(2, 4)),
                    ('combat_events', random.randint(2, 5)),
                    ('customization', random.randint(0, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.70,
                'revenue_range': (0, 20)
            },
            'event_participation': {
                'primary_goal': 'event_completion',
                'base_flow': [
                    ('session_start', 1),
                    ('meta_progression', random.randint(2, 4)),
                    ('gameplay_core', random.randint(3, 6)),
                    ('combat_events', random.randint(2, 5)),
                    ('item_collection', random.randint(1, 3)),
                    ('monetization', random.randint(0, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.55,
                'revenue_range': (0, 30)
            },
            'whale_spending_session': {
                'primary_goal': 'high_value_purchase',
                'base_flow': [
                    ('session_start', 1),
                    ('store_browsing', random.randint(2, 4)),
                    ('monetization', random.randint(3, 7)),
                    ('customization', random.randint(2, 4)),
                    ('gameplay_core', random.randint(1, 3)),
                    ('character_progression', random.randint(1, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.80,
                'revenue_range': (50, 500)
            },
            'tutorial_dropout': {
                'primary_goal': 'early_exit',
                'base_flow': [
                    ('session_start', 1),
                    ('gameplay_core', random.randint(1, 3)),
                    ('combat_events', random.randint(0, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.05,
                'revenue_range': (0, 0)
            },
            'return_player_session': {
                'primary_goal': 're_engagement',
                'base_flow': [
                    ('session_start', 1),
                    ('meta_progression', random.randint(1, 2)),
                    ('store_browsing', random.randint(0, 2)),
                    ('gameplay_core', random.randint(2, 5)),
                    ('character_progression', random.randint(1, 3)),
                    ('monetization', random.randint(0, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.40,
                'revenue_range': (0, 40)
            }
        }
        
        # Detailed event mappings for gaming
        event_details = {
            # Session Start
            'game_launch': {'name': 'Game Launch', 'url': '/game/launch', 'type': 'system', 'category': 'session'},
            'login_success': {'name': 'Login Success', 'url': '/login/success', 'type': 'authentication', 'category': 'session'},
            'main_menu_enter': {'name': 'Main Menu', 'url': '/menu/main', 'type': 'navigation', 'category': 'session'},
            'tutorial_start': {'name': 'Tutorial Start', 'url': '/tutorial/start', 'type': 'onboarding', 'category': 'session'},
            'continue_game': {'name': 'Continue Game', 'url': '/game/continue', 'type': 'navigation', 'category': 'session'},
            'new_game_start': {'name': 'New Game', 'url': '/game/new', 'type': 'navigation', 'category': 'session'},
            'profile_load': {'name': 'Profile Load', 'url': '/profile/load', 'type': 'system', 'category': 'session'},
            'settings_check': {'name': 'Settings Check', 'url': '/settings', 'type': 'navigation', 'category': 'session'},
            
            # Gameplay Core
            'level_start': {'name': 'Level Start', 'url': '/level/start', 'type': 'gameplay', 'category': 'core'},
            'level_complete': {'name': 'Level Complete', 'url': '/level/complete', 'type': 'gameplay', 'category': 'core'},
            'level_failed': {'name': 'Level Failed', 'url': '/level/failed', 'type': 'gameplay', 'category': 'core'},
            'checkpoint_reached': {'name': 'Checkpoint Reached', 'url': '/checkpoint', 'type': 'gameplay', 'category': 'core'},
            'boss_encounter': {'name': 'Boss Encounter', 'url': '/boss/encounter', 'type': 'gameplay', 'category': 'core'},
            'boss_defeated': {'name': 'Boss Defeated', 'url': '/boss/defeated', 'type': 'gameplay', 'category': 'core'},
            'mission_start': {'name': 'Mission Start', 'url': '/mission/start', 'type': 'gameplay', 'category': 'core'},
            'mission_complete': {'name': 'Mission Complete', 'url': '/mission/complete', 'type': 'gameplay', 'category': 'core'},
            'quest_accepted': {'name': 'Quest Accepted', 'url': '/quest/accept', 'type': 'gameplay', 'category': 'core'},
            'quest_completed': {'name': 'Quest Completed', 'url': '/quest/complete', 'type': 'gameplay', 'category': 'core'},
            'objective_completed': {'name': 'Objective Complete', 'url': '/objective/complete', 'type': 'gameplay', 'category': 'core'},
            
            # Character Progression
            'level_up': {'name': 'Level Up', 'url': '/character/levelup', 'type': 'progression', 'category': 'character'},
            'xp_gained': {'name': 'XP Gained', 'url': '/character/xp', 'type': 'progression', 'category': 'character'},
            'skill_unlocked': {'name': 'Skill Unlocked', 'url': '/character/skill', 'type': 'progression', 'category': 'character'},
            'ability_upgraded': {'name': 'Ability Upgraded', 'url': '/character/ability', 'type': 'progression', 'category': 'character'},
            'stat_increased': {'name': 'Stat Increased', 'url': '/character/stats', 'type': 'progression', 'category': 'character'},
            'character_created': {'name': 'Character Created', 'url': '/character/create', 'type': 'progression', 'category': 'character'},
            'class_selected': {'name': 'Class Selected', 'url': '/character/class', 'type': 'progression', 'category': 'character'},
            'talent_point_spent': {'name': 'Talent Point Spent', 'url': '/character/talent', 'type': 'progression', 'category': 'character'},
            'prestige_reached': {'name': 'Prestige Reached', 'url': '/character/prestige', 'type': 'progression', 'category': 'character'},
            'achievement_earned': {'name': 'Achievement Earned', 'url': '/achievement', 'type': 'progression', 'category': 'character'},
            
            # Combat Events
            'enemy_killed': {'name': 'Enemy Killed', 'url': '/combat/kill', 'type': 'combat', 'category': 'combat'},
            'player_death': {'name': 'Player Death', 'url': '/combat/death', 'type': 'combat', 'category': 'combat'},
            'damage_dealt': {'name': 'Damage Dealt', 'url': '/combat/damage_out', 'type': 'combat', 'category': 'combat'},
            'damage_taken': {'name': 'Damage Taken', 'url': '/combat/damage_in', 'type': 'combat', 'category': 'combat'},
            'critical_hit': {'name': 'Critical Hit', 'url': '/combat/critical', 'type': 'combat', 'category': 'combat'},
            'combo_executed': {'name': 'Combo Executed', 'url': '/combat/combo', 'type': 'combat', 'category': 'combat'},
            'spell_cast': {'name': 'Spell Cast', 'url': '/combat/spell', 'type': 'combat', 'category': 'combat'},
            'item_used_combat': {'name': 'Item Used in Combat', 'url': '/combat/item', 'type': 'combat', 'category': 'combat'},
            'weapon_equipped': {'name': 'Weapon Equipped', 'url': '/equipment/weapon', 'type': 'equipment', 'category': 'combat'},
            'armor_equipped': {'name': 'Armor Equipped', 'url': '/equipment/armor', 'type': 'equipment', 'category': 'combat'},
            
            # Item Collection
            'item_found': {'name': 'Item Found', 'url': '/loot/item', 'type': 'collection', 'category': 'items'},
            'treasure_opened': {'name': 'Treasure Opened', 'url': '/loot/treasure', 'type': 'collection', 'category': 'items'},
            'loot_collected': {'name': 'Loot Collected', 'url': '/loot/collect', 'type': 'collection', 'category': 'items'},
            'rare_drop': {'name': 'Rare Drop', 'url': '/loot/rare', 'type': 'collection', 'category': 'items'},
            'crafting_material_found': {'name': 'Crafting Material', 'url': '/loot/material', 'type': 'collection', 'category': 'items'},
            'currency_found': {'name': 'Currency Found', 'url': '/loot/currency', 'type': 'collection', 'category': 'items'},
            'item_crafted': {'name': 'Item Crafted', 'url': '/crafting/craft', 'type': 'crafting', 'category': 'items'},
            'equipment_upgraded': {'name': 'Equipment Upgraded', 'url': '/crafting/upgrade', 'type': 'crafting', 'category': 'items'},
            'inventory_full': {'name': 'Inventory Full', 'url': '/inventory/full', 'type': 'inventory', 'category': 'items'},
            'item_sold': {'name': 'Item Sold', 'url': '/inventory/sell', 'type': 'economy', 'category': 'items'},
            
            # Social & Multiplayer
            'friend_added': {'name': 'Friend Added', 'url': '/social/friend', 'type': 'social', 'category': 'social'},
            'party_joined': {'name': 'Party Joined', 'url': '/social/party', 'type': 'social', 'category': 'social'},
            'guild_joined': {'name': 'Guild Joined', 'url': '/social/guild', 'type': 'social', 'category': 'social'},
            'chat_message_sent': {'name': 'Chat Message', 'url': '/social/chat', 'type': 'social', 'category': 'social'},
            'voice_chat_started': {'name': 'Voice Chat', 'url': '/social/voice', 'type': 'social', 'category': 'social'},
            'player_invited': {'name': 'Player Invited', 'url': '/social/invite', 'type': 'social', 'category': 'social'},
            'match_found': {'name': 'Match Found', 'url': '/matchmaking/found', 'type': 'matchmaking', 'category': 'social'},
            'team_formed': {'name': 'Team Formed', 'url': '/matchmaking/team', 'type': 'matchmaking', 'category': 'social'},
            'leaderboard_viewed': {'name': 'Leaderboard Viewed', 'url': '/leaderboard', 'type': 'competitive', 'category': 'social'},
            'tournament_entered': {'name': 'Tournament Entered', 'url': '/tournament/enter', 'type': 'competitive', 'category': 'social'},
            
            # Store Browsing
            'store_opened': {'name': 'Store Opened', 'url': '/store', 'type': 'store', 'category': 'monetization'},
            'category_browsed': {'name': 'Category Browsed', 'url': '/store/category', 'type': 'store', 'category': 'monetization'},
            'item_previewed': {'name': 'Item Previewed', 'url': '/store/preview', 'type': 'store', 'category': 'monetization'},
            'item_details_viewed': {'name': 'Item Details', 'url': '/store/details', 'type': 'store', 'category': 'monetization'},
            'price_checked': {'name': 'Price Checked', 'url': '/store/price', 'type': 'store', 'category': 'monetization'},
            'bundle_viewed': {'name': 'Bundle Viewed', 'url': '/store/bundle', 'type': 'store', 'category': 'monetization'},
            'sale_items_browsed': {'name': 'Sale Items', 'url': '/store/sale', 'type': 'store', 'category': 'monetization'},
            'wishlist_viewed': {'name': 'Wishlist Viewed', 'url': '/store/wishlist', 'type': 'store', 'category': 'monetization'},
            'recommendations_viewed': {'name': 'Recommendations', 'url': '/store/recommended', 'type': 'store', 'category': 'monetization'},
            'search_store': {'name': 'Store Search', 'url': '/store/search', 'type': 'store', 'category': 'monetization'},
            
            # Monetization
            'item_purchased': {'name': 'Item Purchased', 'url': '/store/purchase', 'type': 'purchase', 'category': 'monetization'},
            'bundle_purchased': {'name': 'Bundle Purchased', 'url': '/store/bundle_buy', 'type': 'purchase', 'category': 'monetization'},
            'currency_purchased': {'name': 'Currency Purchased', 'url': '/store/currency', 'type': 'purchase', 'category': 'monetization'},
            'premium_upgrade': {'name': 'Premium Upgrade', 'url': '/store/premium', 'type': 'purchase', 'category': 'monetization'},
            'battle_pass_purchased': {'name': 'Battle Pass', 'url': '/store/battlepass', 'type': 'purchase', 'category': 'monetization'},
            'dlc_purchased': {'name': 'DLC Purchased', 'url': '/store/dlc', 'type': 'purchase', 'category': 'monetization'},
            'cosmetic_purchased': {'name': 'Cosmetic Purchased', 'url': '/store/cosmetic', 'type': 'purchase', 'category': 'monetization'},
            'booster_purchased': {'name': 'Booster Purchased', 'url': '/store/booster', 'type': 'purchase', 'category': 'monetization'},
            'subscription_activated': {'name': 'Subscription Active', 'url': '/store/subscription', 'type': 'purchase', 'category': 'monetization'},
            'gift_purchased': {'name': 'Gift Purchased', 'url': '/store/gift', 'type': 'purchase', 'category': 'monetization'},
            
            # Customization
            'character_customized': {'name': 'Character Customized', 'url': '/customize/character', 'type': 'customization', 'category': 'customization'},
            'outfit_changed': {'name': 'Outfit Changed', 'url': '/customize/outfit', 'type': 'customization', 'category': 'customization'},
            'weapon_skin_applied': {'name': 'Weapon Skin Applied', 'url': '/customize/weapon', 'type': 'customization', 'category': 'customization'},
            'base_decorated': {'name': 'Base Decorated', 'url': '/customize/base', 'type': 'customization', 'category': 'customization'},
            'avatar_updated': {'name': 'Avatar Updated', 'url': '/customize/avatar', 'type': 'customization', 'category': 'customization'},
            'title_changed': {'name': 'Title Changed', 'url': '/customize/title', 'type': 'customization', 'category': 'customization'},
            'emblem_selected': {'name': 'Emblem Selected', 'url': '/customize/emblem', 'type': 'customization', 'category': 'customization'},
            'emote_equipped': {'name': 'Emote Equipped', 'url': '/customize/emote', 'type': 'customization', 'category': 'customization'},
            'victory_pose_set': {'name': 'Victory Pose Set', 'url': '/customize/victory', 'type': 'customization', 'category': 'customization'},
            'loadout_saved': {'name': 'Loadout Saved', 'url': '/customize/loadout', 'type': 'customization', 'category': 'customization'},
            
            # Meta Progression
            'daily_quest_completed': {'name': 'Daily Quest Complete', 'url': '/meta/daily', 'type': 'meta_progression', 'category': 'meta'},
            'weekly_challenge_finished': {'name': 'Weekly Challenge', 'url': '/meta/weekly', 'type': 'meta_progression', 'category': 'meta'},
            'event_participated': {'name': 'Event Participated', 'url': '/meta/event', 'type': 'meta_progression', 'category': 'meta'},
            'seasonal_reward_claimed': {'name': 'Seasonal Reward', 'url': '/meta/seasonal', 'type': 'meta_progression', 'category': 'meta'},
            'battle_pass_tier_unlocked': {'name': 'Battle Pass Tier', 'url': '/meta/battlepass_tier', 'type': 'meta_progression', 'category': 'meta'},
            'login_bonus_claimed': {'name': 'Login Bonus', 'url': '/meta/login_bonus', 'type': 'meta_progression', 'category': 'meta'},
            'milestone_reached': {'name': 'Milestone Reached', 'url': '/meta/milestone', 'type': 'meta_progression', 'category': 'meta'},
            'collection_completed': {'name': 'Collection Complete', 'url': '/meta/collection', 'type': 'meta_progression', 'category': 'meta'},
            'mastery_achieved': {'name': 'Mastery Achieved', 'url': '/meta/mastery', 'type': 'meta_progression', 'category': 'meta'},
            
            # Session End
            'game_paused': {'name': 'Game Paused', 'url': '/game/pause', 'type': 'system', 'category': 'session_end'},
            'settings_accessed': {'name': 'Settings Accessed', 'url': '/settings', 'type': 'navigation', 'category': 'session_end'},
            'save_game': {'name': 'Game Saved', 'url': '/game/save', 'type': 'system', 'category': 'session_end'},
            'logout': {'name': 'Logout', 'url': '/logout', 'type': 'authentication', 'category': 'session_end'},
            'session_timeout': {'name': 'Session Timeout', 'url': '/timeout', 'type': 'system', 'category': 'session_end'},
            'connection_lost': {'name': 'Connection Lost', 'url': '/disconnect', 'type': 'system', 'category': 'session_end'},
            'game_closed': {'name': 'Game Closed', 'url': '/game/close', 'type': 'system', 'category': 'session_end'},
            'platform_exit': {'name': 'Platform Exit', 'url': '/platform/exit', 'type': 'system', 'category': 'session_end'}
        }
        
        # Gaming data
        game_titles = [
            'Epic Quest Chronicles', 'Battle Royale Arena', 'Space Marine Command',
            'Fantasy Realm Adventures', 'City Builder Tycoon', 'Racing Champions',
            'Puzzle Master Pro', 'Card Legends', 'Tower Defense Elite',
            'MMORPG Worlds', 'First Person Shooter', 'Strategy Empire',
            'Platform Hero', 'Fighting Tournament', 'Survival Island'
        ]
        
        game_modes = [
            'single_player', 'multiplayer', 'co_op', 'competitive', 'ranked',
            'casual', 'tutorial', 'practice', 'campaign', 'survival',
            'battle_royale', 'team_deathmatch', 'capture_flag', 'domination'
        ]
        
        platforms = [
            'PC_Steam', 'PC_Epic', 'PlayStation_5', 'PlayStation_4', 'Xbox_Series_X',
            'Xbox_One', 'Nintendo_Switch', 'iOS', 'Android', 'Web_Browser'
        ]
        
        character_classes = [
            'Warrior', 'Mage', 'Archer', 'Rogue', 'Paladin', 'Necromancer',
            'Healer', 'Tank', 'Support', 'Assassin', 'Berserker', 'Shaman'
        ]
        
        item_categories = [
            'weapons', 'armor', 'cosmetics', 'consumables', 'currency',
            'boosters', 'battle_pass', 'dlc', 'character_packs', 'emotes'
        ]
        
        item_rarities = ['common', 'uncommon', 'rare', 'epic', 'legendary', 'mythic']
        
        currency_types = ['gold', 'gems', 'coins', 'crystals', 'premium_currency', 'real_money']
        
        player_segments = [
            'new_player', 'casual_gamer', 'core_gamer', 'hardcore_gamer',
            'competitive_player', 'social_player', 'whale_spender', 'content_creator'
        ]
        
        spending_tiers = ['free_to_play', 'low_spender', 'moderate_spender', 'high_spender', 'whale']
        skill_levels = ['beginner', 'novice', 'intermediate', 'advanced', 'expert', 'pro']
        playtime_categories = ['casual', 'regular', 'frequent', 'heavy', 'addicted']
        social_activity_levels = ['solo', 'occasional', 'social', 'very_social', 'community_leader']
        
        device_types = ['Desktop', 'Mobile', 'Console', 'Tablet']
        operating_systems = [
            'Windows 11', 'Windows 10', 'macOS 14', 'macOS 13',
            'iOS 17', 'iOS 16', 'Android 14', 'Android 13',
            'PlayStation OS', 'Xbox OS', 'Nintendo OS'
        ]
        connection_types = ['wifi', 'ethernet', 'cellular_5g', 'cellular_4g', 'cellular_3g']
        
        # Generate consistent user profile for this journey
        user_id = str(uuid.uuid4())
        player_id = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        
        player_segment = random.choice(player_segments)
        spending_tier = random.choice(spending_tiers)
        skill_level = random.choice(skill_levels)
        playtime_category = random.choice(playtime_categories)
        social_activity_level = random.choice(social_activity_levels)
        
        # Consistent game and character data
        game_title = random.choice(game_titles)
        game_mode = random.choice(game_modes)
        platform = random.choice(platforms)
        game_version = f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        character_class = random.choice(character_classes)
        character_level = random.randint(1, 100)
        current_xp = random.randint(0, 10000)
        
        # Technical details
        device_type = random.choice(device_types)
        os = random.choice(operating_systems)
        client_version = f"{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        connection_type = random.choice(connection_types)
        
        # Performance metrics
        fps_average = random.randint(30, 120)
        ping_ms = random.randint(10, 200)
        
        # Geographic data
        country = 'United States'
        region = fake.state()
        timezone = random.choice(['PST', 'MST', 'CST', 'EST'])
        
        # Premium status
        is_premium_player = spending_tier in ['high_spender', 'whale']
        is_first_session = random.random() < 0.1  # 10% are first sessions
        
        # Choose a journey template
        journey_name = random.choice(list(journey_templates.keys()))
        journey_template = journey_templates[journey_name]
        
        # Build the actual event sequence from the template
        event_sequence = []
        for event_category, count in journey_template['base_flow']:
            selected_events = random.sample(shared_events[event_category], min(count, len(shared_events[event_category])))
            event_sequence.extend(selected_events)
        
        # Add some randomization - 15% chance to add extra events
        if random.random() < 0.15:
            extra_categories = [cat for cat in shared_events.keys() if cat not in ['session_end']]
            extra_category = random.choice(extra_categories)
            extra_event = random.choice(shared_events[extra_category])
            insert_pos = random.randint(1, len(event_sequence) - 1)
            event_sequence.insert(insert_pos, extra_event)
        
        # Generate journey start time
        journey_start = datetime.now() - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Generate events for the journey
        converted = False
        total_spent = 0
        session_duration = random.randint(5, 180)  # 5 to 180 minutes
        
        for i, event_type in enumerate(event_sequence):
            # Calculate event timestamp
            if i == 0:
                event_timestamp = journey_start
            else:
                # Gaming events happen more frequently
                gap_seconds = random.randint(10, 300)  # 10 seconds to 5 minutes
                event_timestamp = previous_timestamp + timedelta(seconds=gap_seconds)
            
            previous_timestamp = event_timestamp
            
            # Get event information
            event_info = event_details.get(event_type, {
                'name': event_type.replace('_', ' ').title(),
                'url': f'/{event_type.replace("_", "/")}',
                'type': 'general',
                'category': 'other'
            })
            
            # Level information
            level_names = [
                'Tutorial Zone', 'Forest of Beginnings', 'Dark Cave', 'Mountain Peak',
                'Fire Temple', 'Ice Cavern', 'Sky Castle', 'Final Boss Arena',
                'PvP Arena', 'Raid Dungeon', 'Training Ground', 'Hub World'
            ]
            level_name = random.choice(level_names) if event_info['category'] in ['core', 'combat'] else None
            
            # Gameplay metrics
            actions_per_minute = random.randint(5, 50)
            score_achieved = random.randint(0, 10000) if event_info['category'] in ['core', 'combat'] else None
            
            # Currency and items
            currency_earned = random.randint(0, 500) if event_info['category'] in ['core', 'items'] else None
            currency_spent = 0
            items_collected = None
            achievement_unlocked = None
            
            if event_type in ['item_found', 'loot_collected', 'rare_drop']:
                items_collected = f"{random.choice(['Sword', 'Shield', 'Potion', 'Gem', 'Scroll'])} of {random.choice(['Power', 'Speed', 'Strength', 'Magic', 'Wisdom'])}"
            
            if event_type == 'achievement_earned':
                achievement_unlocked = f"{random.choice(['First', 'Master', 'Elite', 'Legendary'])} {random.choice(['Fighter', 'Explorer', 'Collector', 'Survivor'])}"
            
            # Store purchases
            item_purchased = None
            item_category = None
            item_rarity = None
            purchase_price = None
            purchase_currency_type = None
            revenue_impact = None
            
            if event_info['category'] == 'monetization' and 'purchased' in event_type:
                item_category = random.choice(item_categories)
                item_rarity = random.choice(item_rarities)
                purchase_currency_type = random.choice(currency_types)
                
                # Price based on rarity and category
                base_prices = {
                    'common': 1, 'uncommon': 5, 'rare': 15, 'epic': 25, 'legendary': 50, 'mythic': 100
                }
                category_multipliers = {
                    'weapons': 2.00, 'armor': 1.50, 'cosmetics': 1.00, 'consumables': 0.50,
                    'currency': 1.00, 'boosters': 0.80, 'battle_pass': 3.00, 'dlc': 5.00
                }
                
                purchase_price = base_prices[item_rarity] * category_multipliers.get(item_category, 1.0)
                
                if purchase_currency_type == 'real_money':
                    purchase_price = round(purchase_price, 2)
                    revenue_impact = purchase_price
                    total_spent += purchase_price
                else:
                    purchase_price = int(purchase_price * 100)  # Convert to in-game currency
                    currency_spent = purchase_price
                
                item_purchased = f"{item_rarity.title()} {item_category.replace('_', ' ').title()}"
            
            # Conversion determination
            conversion_events = [
                'item_purchased', 'bundle_purchased', 'premium_upgrade', 'battle_pass_purchased'
            ]
            is_conversion_event = event_type in conversion_events
            
            if is_conversion_event and random.random() < journey_template['conversion_rate']:
                converted = True
            
            # Custom gaming events (counts)
            level_completions = 1 if event_type in ['level_complete', 'mission_complete', 'quest_completed'] else 0
            deaths_count = 1 if event_type == 'player_death' else 0
            kills_count = random.randint(0, 5) if event_type == 'enemy_killed' else 0
            items_used = 1 if 'item_used' in event_type or 'equipped' in event_type else 0
            social_interactions = 1 if event_info['category'] == 'social' else 0
            
            yield (
                # Core identifiers
                str(uuid.uuid4()),  # event_id
                user_id,  # user_id (consistent)
                player_id,  # player_id (consistent)
                session_id,  # session_id (consistent)
                
                # Event details
                event_timestamp,  # event_timestamp
                event_type,  # event_type
                event_info['category'],  # event_category
                event_type.replace('_', ' ').title(),  # event_action
                f"{journey_template['primary_goal']}_{event_type}",  # event_label
                
                # Game/Platform information
                game_title,  # game_title
                game_mode,  # game_mode
                platform,  # platform
                game_version,  # game_version
                level_name,  # level_name
                
                # Technical details
                device_type,  # device_type
                os,  # operating_system
                client_version,  # client_version
                connection_type,  # connection_type
                fps_average,  # fps_average
                ping_ms,  # ping_ms
                
                # Geographic data
                country,  # country
                region,  # region
                timezone,  # timezone
                
                # Gameplay metrics
                session_duration,  # session_duration_minutes
                actions_per_minute,  # actions_per_minute
                score_achieved,  # score_achieved
                
                # Game-specific fields
                character_class,  # character_class
                character_level,  # character_level
                current_xp,  # current_xp
                currency_earned,  # currency_earned
                currency_spent,  # currency_spent
                items_collected,  # items_collected
                achievement_unlocked,  # achievement_unlocked
                
                # Store/Monetization fields
                item_purchased,  # item_purchased
                item_category,  # item_category
                item_rarity,  # item_rarity
                purchase_price,  # purchase_price
                purchase_currency_type,  # currency_type
                total_spent,  # total_spent
                
                # Player behavior dimensions
                player_segment,  # player_segment
                spending_tier,  # spending_tier
                skill_level,  # skill_level
                playtime_category,  # playtime_category
                social_activity_level,  # social_activity_level
                
                # Custom gaming events
                level_completions,  # level_completions
                deaths_count,  # deaths_count
                kills_count,  # kills_count
                items_used,  # items_used
                social_interactions,  # social_interactions
                
                # Additional context
                is_premium_player,  # is_premium_player
                is_first_session,  # is_first_session
                is_conversion_event and converted,  # conversion_flag
                revenue_impact  # revenue_impact
            )
$$;

CREATE OR REPLACE TABLE gaming_events AS
SELECT e.*
FROM TABLE(GENERATOR(ROWCOUNT => $JOURNEY_COUNT)) g
CROSS JOIN TABLE(generate_gaming_journey()) e;

GRANT SELECT ON ALL TABLES IN SCHEMA SEQUENT_DB.GAMING TO ROLE SEQUENT_ROLE;


-- ===========================================================================
-- SECTION 7: GENERATE DELIVERY DATA
-- ===========================================================================

USE SCHEMA SEQUENT_DB.DELIVERY;

CREATE OR REPLACE FUNCTION generate_food_delivery_journey()
RETURNS TABLE (
    -- Core identifiers
    event_id STRING,
    user_id STRING,
    customer_id STRING,
    session_id STRING,
    order_id STRING,
    
    -- Event details
    event_timestamp TIMESTAMP,
    event_type STRING,
    event_category STRING,
    event_action STRING,
    event_label STRING,
    
    -- App/Platform information
    platform STRING,
    app_version STRING,
    device_model STRING,
    operating_system STRING,
    user_agent STRING,
    
    -- Geographic data
    country STRING,
    state STRING,
    city STRING,
    zip_code STRING,
    delivery_zone STRING,
    
    -- Restaurant and food data
    restaurant_name STRING,
    restaurant_category STRING,
    cuisine_type STRING,
    restaurant_rating DECIMAL(3,2),
    delivery_time_estimate INT,
    item_name STRING,
    item_category STRING,
    item_price DECIMAL(8,2),
    
    -- Order details
    order_subtotal DECIMAL(10,2),
    delivery_fee DECIMAL(6,2),
    service_fee DECIMAL(6,2),
    tip_amount DECIMAL(8,2),
    taxes DECIMAL(8,2),
    total_order_value DECIMAL(12,2),
    payment_method STRING,
    
    -- Delivery information
    delivery_address_type STRING,
    estimated_delivery_time INT,
    actual_delivery_time INT,
    delivery_instructions STRING,
    driver_rating DECIMAL(3,2),
    
    -- Marketing and engagement
    campaign_id STRING,
    promo_code_used STRING,
    discount_amount DECIMAL(8,2),
    notification_type STRING,
    email_campaign_name STRING,
    
    -- Customer behavior dimensions
    customer_segment STRING,
    order_frequency_tier STRING,
    spending_tier STRING,
    preferred_cuisine STRING,
    dietary_preferences STRING,
    
    -- Custom food delivery events
    restaurant_views INT,
    menu_item_views INT,
    cart_additions INT,
    order_placements INT,
    reorders INT,
    
    -- Additional context
    is_first_order BOOLEAN,
    is_peak_hours BOOLEAN,
    weather_condition STRING,
    conversion_flag BOOLEAN,
    revenue_impact DECIMAL(12,2)
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'generateJourney'
PACKAGES = ('faker')
AS $$
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

class generateJourney:
    def process(self):
        # Define shared event pools for food delivery experiences
        shared_events = {
            'app_entry': [
                'app_launch', 'push_notification_click', 'email_campaign_click', 'sms_link_click',
                'deeplink_open', 'widget_interaction', 'home_screen_shortcut', 'voice_assistant_open'
            ],
            'authentication': [
                'login_attempt', 'login_success', 'guest_order_start', 'account_creation_start',
                'social_login', 'phone_verification', 'email_verification', 'biometric_login'
            ],
            'location_services': [
                'location_permission_request', 'location_detected', 'address_entry', 'address_selection',
                'address_validation', 'delivery_zone_check', 'location_update', 'favorite_address_select'
            ],
            'restaurant_discovery': [
                'restaurant_feed_view', 'category_browse', 'cuisine_filter', 'rating_filter',
                'distance_filter', 'delivery_time_filter', 'price_range_filter', 'search_restaurants',
                'featured_restaurants_view', 'nearby_restaurants_view', 'trending_restaurants_view'
            ],
            'restaurant_interaction': [
                'restaurant_profile_view', 'menu_browse', 'item_details_view', 'photo_gallery_view',
                'reviews_section_view', 'restaurant_info_view', 'hours_check', 'contact_info_view',
                'favorite_restaurant_add', 'share_restaurant'
            ],
            'menu_navigation': [
                'menu_category_select', 'item_search', 'item_filter_apply', 'popular_items_view',
                'recommended_items_view', 'combo_deals_view', 'add_ons_view', 'customization_options',
                'nutritional_info_view', 'allergen_info_view'
            ],
            'cart_management': [
                'add_to_cart', 'cart_view', 'quantity_update', 'item_remove', 'item_customize',
                'special_instructions_add', 'cart_save_later', 'cart_share', 'similar_items_view',
                'upsell_item_view'
            ],
            'checkout_process': [
                'checkout_initiate', 'delivery_time_select', 'payment_method_select', 'tip_amount_select',
                'promo_code_apply', 'order_review', 'order_confirmation', 'payment_processing',
                'order_placed_success', 'receipt_view'
            ],
            'order_tracking': [
                'order_status_check', 'restaurant_preparing', 'driver_assigned', 'driver_pickup',
                'delivery_in_progress', 'delivery_eta_update', 'driver_location_track', 'delivery_arrived',
                'order_delivered', 'delivery_photo_view'
            ],
            'rating_feedback': [
                'restaurant_rating', 'driver_rating', 'order_rating', 'review_submission',
                'photo_review_upload', 'feedback_survey', 'complaint_submission', 'compliment_submission'
            ],
            'loyalty_rewards': [
                'loyalty_points_check', 'rewards_catalog_view', 'points_redemption', 'tier_status_check',
                'cashback_view', 'referral_program_use', 'milestone_achievement', 'bonus_points_earned'
            ],
            'customer_service': [
                'help_center_view', 'faq_browse', 'live_chat_start', 'call_support_request',
                'order_issue_report', 'refund_request', 'driver_feedback', 'restaurant_complaint',
                'missing_items_report', 'delivery_delay_report'
            ],
            'marketing_engagement': [
                'push_notification_receive', 'email_open', 'sms_receive', 'in_app_banner_click',
                'flash_sale_view', 'daily_deal_view', 'personalized_offer_view', 'group_order_invite',
                'social_share_deal', 'newsletter_signup'
            ],
            'social_features': [
                'group_order_create', 'group_order_join', 'friends_orders_view', 'social_feed_view',
                'restaurant_recommendation_send', 'order_history_share', 'wishlist_create',
                'follow_friends', 'create_food_list', 'join_food_challenge'
            ],
            'account_management': [
                'profile_update', 'payment_methods_manage', 'addresses_manage', 'preferences_update',
                'order_history_view', 'favorite_restaurants_view', 'dietary_preferences_set',
                'notification_settings', 'privacy_settings', 'subscription_manage'
            ],
            'session_end': [
                'app_minimize', 'logout', 'session_timeout', 'app_crash', 'network_disconnect',
                'background_mode', 'force_close', 'natural_exit'
            ]
        }
        
        # Define journey templates for food delivery experiences
        journey_templates = {
            'first_time_user_order': {
                'primary_goal': 'first_order',
                'base_flow': [
                    ('app_entry', 1),
                    ('authentication', random.randint(1, 2)),
                    ('location_services', random.randint(1, 3)),
                    ('restaurant_discovery', random.randint(3, 6)),
                    ('restaurant_interaction', random.randint(2, 4)),
                    ('menu_navigation', random.randint(2, 5)),
                    ('cart_management', random.randint(2, 4)),
                    ('checkout_process', random.randint(4, 8)),
                    ('order_tracking', random.randint(2, 4)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.25,
                'revenue_range': (15, 45)
            },
            'regular_customer_reorder': {
                'primary_goal': 'repeat_order',
                'base_flow': [
                    ('app_entry', 1),
                    ('authentication', random.randint(0, 1)),
                    ('restaurant_discovery', random.randint(1, 2)),
                    ('restaurant_interaction', random.randint(1, 2)),
                    ('menu_navigation', random.randint(1, 3)),
                    ('cart_management', random.randint(1, 2)),
                    ('checkout_process', random.randint(3, 5)),
                    ('order_tracking', random.randint(2, 4)),
                    ('rating_feedback', random.randint(0, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.65,
                'revenue_range': (20, 60)
            },
            'deal_hunting_session': {
                'primary_goal': 'deal_order',
                'base_flow': [
                    ('app_entry', 1),
                    ('marketing_engagement', random.randint(1, 3)),
                    ('restaurant_discovery', random.randint(2, 4)),
                    ('restaurant_interaction', random.randint(2, 4)),
                    ('menu_navigation', random.randint(1, 3)),
                    ('cart_management', random.randint(1, 3)),
                    ('checkout_process', random.randint(3, 6)),
                    ('order_tracking', random.randint(1, 3)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.45,
                'revenue_range': (12, 35)
            },
            'browsing_no_order': {
                'primary_goal': 'exploration',
                'base_flow': [
                    ('app_entry', 1),
                    ('authentication', random.randint(0, 1)),
                    ('location_services', random.randint(0, 1)),
                    ('restaurant_discovery', random.randint(3, 7)),
                    ('restaurant_interaction', random.randint(2, 5)),
                    ('menu_navigation', random.randint(1, 4)),
                    ('cart_management', random.randint(0, 2)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.05,
                'revenue_range': (0, 0)
            },
            'group_order_coordination': {
                'primary_goal': 'group_order',
                'base_flow': [
                    ('app_entry', 1),
                    ('authentication', 1),
                    ('social_features', random.randint(2, 4)),
                    ('restaurant_discovery', random.randint(1, 3)),
                    ('restaurant_interaction', random.randint(1, 2)),
                    ('menu_navigation', random.randint(2, 4)),
                    ('cart_management', random.randint(2, 4)),
                    ('checkout_process', random.randint(4, 7)),
                    ('order_tracking', random.randint(2, 4)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.55,
                'revenue_range': (40, 120)
            },
            'premium_dining_experience': {
                'primary_goal': 'premium_order',
                'base_flow': [
                    ('app_entry', 1),
                    ('authentication', 1),
                    ('restaurant_discovery', random.randint(2, 4)),
                    ('restaurant_interaction', random.randint(3, 6)),
                    ('menu_navigation', random.randint(2, 5)),
                    ('cart_management', random.randint(2, 4)),
                    ('checkout_process', random.randint(4, 6)),
                    ('order_tracking', random.randint(3, 5)),
                    ('rating_feedback', random.randint(1, 3)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.70,
                'revenue_range': (35, 100)
            },
            'loyalty_member_session': {
                'primary_goal': 'loyalty_order',
                'base_flow': [
                    ('app_entry', 1),
                    ('authentication', 1),
                    ('loyalty_rewards', random.randint(1, 3)),
                    ('restaurant_discovery', random.randint(1, 3)),
                    ('restaurant_interaction', random.randint(1, 3)),
                    ('menu_navigation', random.randint(1, 3)),
                    ('cart_management', random.randint(1, 2)),
                    ('checkout_process', random.randint(3, 5)),
                    ('order_tracking', random.randint(2, 3)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.75,
                'revenue_range': (18, 55)
            },
            'customer_service_interaction': {
                'primary_goal': 'issue_resolution',
                'base_flow': [
                    ('app_entry', 1),
                    ('authentication', random.randint(0, 1)),
                    ('account_management', random.randint(1, 2)),
                    ('customer_service', random.randint(3, 6)),
                    ('order_tracking', random.randint(0, 2)),
                    ('rating_feedback', random.randint(0, 1)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.30,
                'revenue_range': (0, 25)
            },
            'marketing_response_order': {
                'primary_goal': 'campaign_conversion',
                'base_flow': [
                    ('marketing_engagement', 1),
                    ('app_entry', 1),
                    ('authentication', random.randint(0, 1)),
                    ('restaurant_discovery', random.randint(1, 2)),
                    ('restaurant_interaction', random.randint(1, 3)),
                    ('menu_navigation', random.randint(1, 3)),
                    ('cart_management', random.randint(1, 3)),
                    ('checkout_process', random.randint(3, 6)),
                    ('order_tracking', random.randint(1, 3)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.40,
                'revenue_range': (16, 48)
            },
            'late_night_craving': {
                'primary_goal': 'late_night_order',
                'base_flow': [
                    ('app_entry', 1),
                    ('authentication', random.randint(0, 1)),
                    ('restaurant_discovery', random.randint(1, 3)),
                    ('restaurant_interaction', random.randint(1, 2)),
                    ('menu_navigation', random.randint(1, 2)),
                    ('cart_management', random.randint(1, 2)),
                    ('checkout_process', random.randint(3, 5)),
                    ('order_tracking', random.randint(2, 4)),
                    ('session_end', 1)
                ],
                'conversion_rate': 0.50,
                'revenue_range': (8, 30)
            }
        }
        
        # Detailed event mappings for food delivery
        event_details = {
            # App Entry
            'app_launch': {'name': 'App Launch', 'url': '/app/launch', 'type': 'system', 'category': 'app_entry'},
            'push_notification_click': {'name': 'Push Notification Click', 'url': '/notification/click', 'type': 'marketing', 'category': 'app_entry'},
            'email_campaign_click': {'name': 'Email Campaign Click', 'url': '/email/click', 'type': 'marketing', 'category': 'app_entry'},
            'sms_link_click': {'name': 'SMS Link Click', 'url': '/sms/click', 'type': 'marketing', 'category': 'app_entry'},
            'deeplink_open': {'name': 'Deeplink Open', 'url': '/deeplink', 'type': 'system', 'category': 'app_entry'},
            'widget_interaction': {'name': 'Widget Interaction', 'url': '/widget', 'type': 'system', 'category': 'app_entry'},
            'home_screen_shortcut': {'name': 'Home Screen Shortcut', 'url': '/shortcut', 'type': 'system', 'category': 'app_entry'},
            'voice_assistant_open': {'name': 'Voice Assistant Open', 'url': '/voice', 'type': 'system', 'category': 'app_entry'},
            
            # Authentication
            'login_attempt': {'name': 'Login Attempt', 'url': '/auth/login', 'type': 'authentication', 'category': 'auth'},
            'login_success': {'name': 'Login Success', 'url': '/auth/success', 'type': 'authentication', 'category': 'auth'},
            'guest_order_start': {'name': 'Guest Order Start', 'url': '/auth/guest', 'type': 'authentication', 'category': 'auth'},
            'account_creation_start': {'name': 'Account Creation', 'url': '/auth/register', 'type': 'authentication', 'category': 'auth'},
            'social_login': {'name': 'Social Login', 'url': '/auth/social', 'type': 'authentication', 'category': 'auth'},
            'phone_verification': {'name': 'Phone Verification', 'url': '/auth/phone', 'type': 'authentication', 'category': 'auth'},
            'email_verification': {'name': 'Email Verification', 'url': '/auth/email', 'type': 'authentication', 'category': 'auth'},
            'biometric_login': {'name': 'Biometric Login', 'url': '/auth/biometric', 'type': 'authentication', 'category': 'auth'},
            
            # Location Services
            'location_permission_request': {'name': 'Location Permission', 'url': '/location/permission', 'type': 'system', 'category': 'location'},
            'location_detected': {'name': 'Location Detected', 'url': '/location/detected', 'type': 'system', 'category': 'location'},
            'address_entry': {'name': 'Address Entry', 'url': '/location/address', 'type': 'input', 'category': 'location'},
            'address_selection': {'name': 'Address Selection', 'url': '/location/select', 'type': 'selection', 'category': 'location'},
            'address_validation': {'name': 'Address Validation', 'url': '/location/validate', 'type': 'system', 'category': 'location'},
            'delivery_zone_check': {'name': 'Delivery Zone Check', 'url': '/location/zone', 'type': 'system', 'category': 'location'},
            'location_update': {'name': 'Location Update', 'url': '/location/update', 'type': 'input', 'category': 'location'},
            'favorite_address_select': {'name': 'Favorite Address Select', 'url': '/location/favorite', 'type': 'selection', 'category': 'location'},
            
            # Restaurant Discovery
            'restaurant_feed_view': {'name': 'Restaurant Feed', 'url': '/restaurants/feed', 'type': 'browse', 'category': 'discovery'},
            'category_browse': {'name': 'Category Browse', 'url': '/restaurants/category', 'type': 'browse', 'category': 'discovery'},
            'cuisine_filter': {'name': 'Cuisine Filter', 'url': '/restaurants/filter/cuisine', 'type': 'filter', 'category': 'discovery'},
            'rating_filter': {'name': 'Rating Filter', 'url': '/restaurants/filter/rating', 'type': 'filter', 'category': 'discovery'},
            'distance_filter': {'name': 'Distance Filter', 'url': '/restaurants/filter/distance', 'type': 'filter', 'category': 'discovery'},
            'delivery_time_filter': {'name': 'Delivery Time Filter', 'url': '/restaurants/filter/time', 'type': 'filter', 'category': 'discovery'},
            'price_range_filter': {'name': 'Price Range Filter', 'url': '/restaurants/filter/price', 'type': 'filter', 'category': 'discovery'},
            'search_restaurants': {'name': 'Search Restaurants', 'url': '/restaurants/search', 'type': 'search', 'category': 'discovery'},
            'featured_restaurants_view': {'name': 'Featured Restaurants', 'url': '/restaurants/featured', 'type': 'browse', 'category': 'discovery'},
            'nearby_restaurants_view': {'name': 'Nearby Restaurants', 'url': '/restaurants/nearby', 'type': 'browse', 'category': 'discovery'},
            'trending_restaurants_view': {'name': 'Trending Restaurants', 'url': '/restaurants/trending', 'type': 'browse', 'category': 'discovery'},
            
            # Restaurant Interaction
            'restaurant_profile_view': {'name': 'Restaurant Profile', 'url': '/restaurant/profile', 'type': 'view', 'category': 'restaurant'},
            'menu_browse': {'name': 'Menu Browse', 'url': '/restaurant/menu', 'type': 'browse', 'category': 'restaurant'},
            'item_details_view': {'name': 'Item Details', 'url': '/restaurant/item', 'type': 'view', 'category': 'restaurant'},
            'photo_gallery_view': {'name': 'Photo Gallery', 'url': '/restaurant/photos', 'type': 'view', 'category': 'restaurant'},
            'reviews_section_view': {'name': 'Reviews Section', 'url': '/restaurant/reviews', 'type': 'view', 'category': 'restaurant'},
            'restaurant_info_view': {'name': 'Restaurant Info', 'url': '/restaurant/info', 'type': 'view', 'category': 'restaurant'},
            'hours_check': {'name': 'Hours Check', 'url': '/restaurant/hours', 'type': 'view', 'category': 'restaurant'},
            'contact_info_view': {'name': 'Contact Info', 'url': '/restaurant/contact', 'type': 'view', 'category': 'restaurant'},
            'favorite_restaurant_add': {'name': 'Add to Favorites', 'url': '/restaurant/favorite', 'type': 'action', 'category': 'restaurant'},
            'share_restaurant': {'name': 'Share Restaurant', 'url': '/restaurant/share', 'type': 'social', 'category': 'restaurant'},
            
            # Menu Navigation
            'menu_category_select': {'name': 'Menu Category Select', 'url': '/menu/category', 'type': 'navigation', 'category': 'menu'},
            'item_search': {'name': 'Item Search', 'url': '/menu/search', 'type': 'search', 'category': 'menu'},
            'item_filter_apply': {'name': 'Item Filter', 'url': '/menu/filter', 'type': 'filter', 'category': 'menu'},
            'popular_items_view': {'name': 'Popular Items', 'url': '/menu/popular', 'type': 'view', 'category': 'menu'},
            'recommended_items_view': {'name': 'Recommended Items', 'url': '/menu/recommended', 'type': 'view', 'category': 'menu'},
            'combo_deals_view': {'name': 'Combo Deals', 'url': '/menu/combos', 'type': 'view', 'category': 'menu'},
            'add_ons_view': {'name': 'Add-ons View', 'url': '/menu/addons', 'type': 'view', 'category': 'menu'},
            'customization_options': {'name': 'Customization Options', 'url': '/menu/customize', 'type': 'view', 'category': 'menu'},
            'nutritional_info_view': {'name': 'Nutritional Info', 'url': '/menu/nutrition', 'type': 'view', 'category': 'menu'},
            'allergen_info_view': {'name': 'Allergen Info', 'url': '/menu/allergens', 'type': 'view', 'category': 'menu'},
            
            # Cart Management
            'add_to_cart': {'name': 'Add to Cart', 'url': '/cart/add', 'type': 'action', 'category': 'cart'},
            'cart_view': {'name': 'Cart View', 'url': '/cart', 'type': 'view', 'category': 'cart'},
            'quantity_update': {'name': 'Quantity Update', 'url': '/cart/quantity', 'type': 'action', 'category': 'cart'},
            'item_remove': {'name': 'Item Remove', 'url': '/cart/remove', 'type': 'action', 'category': 'cart'},
            'item_customize': {'name': 'Item Customize', 'url': '/cart/customize', 'type': 'action', 'category': 'cart'},
            'special_instructions_add': {'name': 'Special Instructions', 'url': '/cart/instructions', 'type': 'input', 'category': 'cart'},
            'cart_save_later': {'name': 'Save Cart for Later', 'url': '/cart/save', 'type': 'action', 'category': 'cart'},
            'cart_share': {'name': 'Share Cart', 'url': '/cart/share', 'type': 'social', 'category': 'cart'},
            'similar_items_view': {'name': 'Similar Items', 'url': '/cart/similar', 'type': 'view', 'category': 'cart'},
            'upsell_item_view': {'name': 'Upsell Items', 'url': '/cart/upsell', 'type': 'view', 'category': 'cart'},
            
            # Checkout Process
            'checkout_initiate': {'name': 'Checkout Start', 'url': '/checkout', 'type': 'action', 'category': 'checkout'},
            'delivery_time_select': {'name': 'Delivery Time Select', 'url': '/checkout/time', 'type': 'selection', 'category': 'checkout'},
            'payment_method_select': {'name': 'Payment Method', 'url': '/checkout/payment', 'type': 'selection', 'category': 'checkout'},
            'tip_amount_select': {'name': 'Tip Amount Select', 'url': '/checkout/tip', 'type': 'selection', 'category': 'checkout'},
            'promo_code_apply': {'name': 'Promo Code Apply', 'url': '/checkout/promo', 'type': 'action', 'category': 'checkout'},
            'order_review': {'name': 'Order Review', 'url': '/checkout/review', 'type': 'view', 'category': 'checkout'},
            'order_confirmation': {'name': 'Order Confirmation', 'url': '/checkout/confirm', 'type': 'action', 'category': 'checkout'},
            'payment_processing': {'name': 'Payment Processing', 'url': '/checkout/process', 'type': 'system', 'category': 'checkout'},
            'order_placed_success': {'name': 'Order Placed', 'url': '/checkout/success', 'type': 'confirmation', 'category': 'checkout'},
            'receipt_view': {'name': 'Receipt View', 'url': '/checkout/receipt', 'type': 'view', 'category': 'checkout'},
            
            # Order Tracking
            'order_status_check': {'name': 'Order Status Check', 'url': '/order/status', 'type': 'view', 'category': 'tracking'},
            'restaurant_preparing': {'name': 'Restaurant Preparing', 'url': '/order/preparing', 'type': 'status', 'category': 'tracking'},
            'driver_assigned': {'name': 'Driver Assigned', 'url': '/order/driver', 'type': 'status', 'category': 'tracking'},
            'driver_pickup': {'name': 'Driver Pickup', 'url': '/order/pickup', 'type': 'status', 'category': 'tracking'},
            'delivery_in_progress': {'name': 'Delivery in Progress', 'url': '/order/delivery', 'type': 'status', 'category': 'tracking'},
            'delivery_eta_update': {'name': 'ETA Update', 'url': '/order/eta', 'type': 'status', 'category': 'tracking'},
            'driver_location_track': {'name': 'Driver Location', 'url': '/order/location', 'type': 'view', 'category': 'tracking'},
            'delivery_arrived': {'name': 'Delivery Arrived', 'url': '/order/arrived', 'type': 'status', 'category': 'tracking'},
            'order_delivered': {'name': 'Order Delivered', 'url': '/order/delivered', 'type': 'confirmation', 'category': 'tracking'},
            'delivery_photo_view': {'name': 'Delivery Photo', 'url': '/order/photo', 'type': 'view', 'category': 'tracking'},
            
            # Rating & Feedback
            'restaurant_rating': {'name': 'Restaurant Rating', 'url': '/feedback/restaurant', 'type': 'rating', 'category': 'feedback'},
            'driver_rating': {'name': 'Driver Rating', 'url': '/feedback/driver', 'type': 'rating', 'category': 'feedback'},
            'order_rating': {'name': 'Order Rating', 'url': '/feedback/order', 'type': 'rating', 'category': 'feedback'},
            'review_submission': {'name': 'Review Submission', 'url': '/feedback/review', 'type': 'input', 'category': 'feedback'},
            'photo_review_upload': {'name': 'Photo Review Upload', 'url': '/feedback/photo', 'type': 'upload', 'category': 'feedback'},
            'feedback_survey': {'name': 'Feedback Survey', 'url': '/feedback/survey', 'type': 'survey', 'category': 'feedback'},
            'complaint_submission': {'name': 'Complaint Submission', 'url': '/feedback/complaint', 'type': 'complaint', 'category': 'feedback'},
            'compliment_submission': {'name': 'Compliment Submission', 'url': '/feedback/compliment', 'type': 'compliment', 'category': 'feedback'},
            
            # Loyalty & Rewards
            'loyalty_points_check': {'name': 'Loyalty Points Check', 'url': '/loyalty/points', 'type': 'view', 'category': 'loyalty'},
            'rewards_catalog_view': {'name': 'Rewards Catalog', 'url': '/loyalty/catalog', 'type': 'view', 'category': 'loyalty'},
            'points_redemption': {'name': 'Points Redemption', 'url': '/loyalty/redeem', 'type': 'action', 'category': 'loyalty'},
            'tier_status_check': {'name': 'Tier Status Check', 'url': '/loyalty/tier', 'type': 'view', 'category': 'loyalty'},
            'cashback_view': {'name': 'Cashback View', 'url': '/loyalty/cashback', 'type': 'view', 'category': 'loyalty'},
            'referral_program_use': {'name': 'Referral Program', 'url': '/loyalty/referral', 'type': 'action', 'category': 'loyalty'},
            'milestone_achievement': {'name': 'Milestone Achievement', 'url': '/loyalty/milestone', 'type': 'achievement', 'category': 'loyalty'},
            'bonus_points_earned': {'name': 'Bonus Points Earned', 'url': '/loyalty/bonus', 'type': 'achievement', 'category': 'loyalty'},
            
            # Customer Service
            'help_center_view': {'name': 'Help Center', 'url': '/support/help', 'type': 'view', 'category': 'support'},
            'faq_browse': {'name': 'FAQ Browse', 'url': '/support/faq', 'type': 'browse', 'category': 'support'},
            'live_chat_start': {'name': 'Live Chat Start', 'url': '/support/chat', 'type': 'action', 'category': 'support'},
            'call_support_request': {'name': 'Call Support Request', 'url': '/support/call', 'type': 'action', 'category': 'support'},
            'order_issue_report': {'name': 'Order Issue Report', 'url': '/support/issue', 'type': 'report', 'category': 'support'},
            'refund_request': {'name': 'Refund Request', 'url': '/support/refund', 'type': 'request', 'category': 'support'},
            'driver_feedback': {'name': 'Driver Feedback', 'url': '/support/driver', 'type': 'feedback', 'category': 'support'},
            'restaurant_complaint': {'name': 'Restaurant Complaint', 'url': '/support/restaurant', 'type': 'complaint', 'category': 'support'},
            'missing_items_report': {'name': 'Missing Items Report', 'url': '/support/missing', 'type': 'report', 'category': 'support'},
            'delivery_delay_report': {'name': 'Delivery Delay Report', 'url': '/support/delay', 'type': 'report', 'category': 'support'},
            
            # Marketing Engagement
            'push_notification_receive': {'name': 'Push Notification Receive', 'url': '/marketing/push', 'type': 'receive', 'category': 'marketing'},
            'email_open': {'name': 'Email Open', 'url': '/marketing/email', 'type': 'open', 'category': 'marketing'},
            'sms_receive': {'name': 'SMS Receive', 'url': '/marketing/sms', 'type': 'receive', 'category': 'marketing'},
            'in_app_banner_click': {'name': 'In-App Banner Click', 'url': '/marketing/banner', 'type': 'click', 'category': 'marketing'},
            'flash_sale_view': {'name': 'Flash Sale View', 'url': '/marketing/flash', 'type': 'view', 'category': 'marketing'},
            'daily_deal_view': {'name': 'Daily Deal View', 'url': '/marketing/daily', 'type': 'view', 'category': 'marketing'},
            'personalized_offer_view': {'name': 'Personalized Offer', 'url': '/marketing/personalized', 'type': 'view', 'category': 'marketing'},
            'group_order_invite': {'name': 'Group Order Invite', 'url': '/marketing/group', 'type': 'invite', 'category': 'marketing'},
            'social_share_deal': {'name': 'Social Share Deal', 'url': '/marketing/share', 'type': 'share', 'category': 'marketing'},
            'newsletter_signup': {'name': 'Newsletter Signup', 'url': '/marketing/newsletter', 'type': 'signup', 'category': 'marketing'},
            
            # Social Features
            'group_order_create': {'name': 'Group Order Create', 'url': '/social/group/create', 'type': 'create', 'category': 'social'},
            'group_order_join': {'name': 'Group Order Join', 'url': '/social/group/join', 'type': 'join', 'category': 'social'},
            'friends_orders_view': {'name': 'Friends Orders View', 'url': '/social/friends', 'type': 'view', 'category': 'social'},
            'social_feed_view': {'name': 'Social Feed View', 'url': '/social/feed', 'type': 'view', 'category': 'social'},
            'restaurant_recommendation_send': {'name': 'Restaurant Recommendation', 'url': '/social/recommend', 'type': 'share', 'category': 'social'},
            'order_history_share': {'name': 'Order History Share', 'url': '/social/history', 'type': 'share', 'category': 'social'},
            'wishlist_create': {'name': 'Wishlist Create', 'url': '/social/wishlist', 'type': 'create', 'category': 'social'},
            'follow_friends': {'name': 'Follow Friends', 'url': '/social/follow', 'type': 'follow', 'category': 'social'},
            'create_food_list': {'name': 'Create Food List', 'url': '/social/list', 'type': 'create', 'category': 'social'},
            'join_food_challenge': {'name': 'Join Food Challenge', 'url': '/social/challenge', 'type': 'join', 'category': 'social'},
            
            # Account Management
            'profile_update': {'name': 'Profile Update', 'url': '/account/profile', 'type': 'update', 'category': 'account'},
            'payment_methods_manage': {'name': 'Payment Methods', 'url': '/account/payment', 'type': 'manage', 'category': 'account'},
            'addresses_manage': {'name': 'Addresses Manage', 'url': '/account/addresses', 'type': 'manage', 'category': 'account'},
            'preferences_update': {'name': 'Preferences Update', 'url': '/account/preferences', 'type': 'update', 'category': 'account'},
            'order_history_view': {'name': 'Order History View', 'url': '/account/orders', 'type': 'view', 'category': 'account'},
            'favorite_restaurants_view': {'name': 'Favorite Restaurants', 'url': '/account/favorites', 'type': 'view', 'category': 'account'},
            'dietary_preferences_set': {'name': 'Dietary Preferences', 'url': '/account/dietary', 'type': 'set', 'category': 'account'},
            'notification_settings': {'name': 'Notification Settings', 'url': '/account/notifications', 'type': 'settings', 'category': 'account'},
            'privacy_settings': {'name': 'Privacy Settings', 'url': '/account/privacy', 'type': 'settings', 'category': 'account'},
            'subscription_manage': {'name': 'Subscription Manage', 'url': '/account/subscription', 'type': 'manage', 'category': 'account'},
            
            # Session End
            'app_minimize': {'name': 'App Minimize', 'url': '/app/minimize', 'type': 'system', 'category': 'session_end'},
            'logout': {'name': 'Logout', 'url': '/auth/logout', 'type': 'authentication', 'category': 'session_end'},
            'session_timeout': {'name': 'Session Timeout', 'url': '/session/timeout', 'type': 'system', 'category': 'session_end'},
            'app_crash': {'name': 'App Crash', 'url': '/app/crash', 'type': 'system', 'category': 'session_end'},
            'network_disconnect': {'name': 'Network Disconnect', 'url': '/network/disconnect', 'type': 'system', 'category': 'session_end'},
            'background_mode': {'name': 'Background Mode', 'url': '/app/background', 'type': 'system', 'category': 'session_end'},
            'force_close': {'name': 'Force Close', 'url': '/app/force_close', 'type': 'system', 'category': 'session_end'},
            'natural_exit': {'name': 'Natural Exit', 'url': '/app/exit', 'type': 'system', 'category': 'session_end'}
        }
        
        # Food delivery data
        restaurant_names = [
            'Pizza Palace', 'Burger Kingdom', 'Taco Fiesta', 'Sushi Zen', 'Noodle House',
            'BBQ Pit Master', 'Green Garden Salads', 'Spice Route Indian', 'Pasta La Vista',
            'Wings & Things', 'Thai Orchid', 'Mediterranean Grill', 'Sandwich Station',
            'Ice Cream Dreams', 'Coffee Corner', 'Breakfast Bistro', 'Seafood Shack',
            'Steakhouse Supreme', 'Vegan Vibes', 'Donut Delight', 'Smoothie Central',
            'Fried Chicken Express', 'Ramen Station', 'Greek Gyros', 'Mexican Cantina'
        ]
        
        restaurant_categories = [
            'Fast Food', 'Fast Casual', 'Casual Dining', 'Fine Dining', 'Coffee & Tea',
            'Desserts', 'Healthy', 'Comfort Food', 'Street Food', 'Bakery'
        ]
        
        cuisine_types = [
            'American', 'Italian', 'Mexican', 'Chinese', 'Japanese', 'Indian', 'Thai',
            'Mediterranean', 'Greek', 'Korean', 'Vietnamese', 'French', 'Spanish',
            'BBQ', 'Seafood', 'Vegetarian', 'Vegan', 'Halal', 'Kosher'
        ]
        
        item_categories = [
            'appetizers', 'entrees', 'sides', 'desserts', 'beverages', 'salads',
            'soups', 'sandwiches', 'pizza', 'pasta', 'burgers', 'tacos'
        ]
        
        payment_methods = [
            'credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay',
            'venmo', 'cash', 'gift_card', 'loyalty_points', 'corporate_card'
        ]
        
        delivery_address_types = ['home', 'work', 'hotel', 'friend', 'other']
        
        customer_segments = [
            'new_user', 'occasional_orderer', 'regular_customer', 'frequent_orderer',
            'premium_customer', 'bargain_hunter', 'food_explorer', 'convenience_seeker'
        ]
        
        order_frequency_tiers = ['first_time', 'occasional', 'regular', 'frequent', 'daily']
        spending_tiers = ['budget', 'moderate', 'premium', 'high_value']
        
        dietary_preferences = [
            'none', 'vegetarian', 'vegan', 'gluten_free', 'keto', 'paleo',
            'dairy_free', 'nut_allergy', 'low_sodium', 'diabetic_friendly'
        ]
        
        platforms = ['iOS', 'Android', 'Web']
        device_models = [
            'iPhone 15', 'iPhone 14', 'iPhone 13', 'Samsung Galaxy S24', 'Samsung Galaxy S23',
            'Google Pixel 8', 'OnePlus 12', 'iPad Pro', 'Samsung Tablet', 'Desktop'
        ]
        operating_systems = ['iOS 17', 'iOS 16', 'Android 14', 'Android 13', 'Windows 11', 'macOS 14']
        
        notification_types = ['push', 'email', 'sms', 'in_app']
        weather_conditions = ['sunny', 'rainy', 'snowy', 'cloudy', 'stormy', 'hot', 'cold']
        
        # Generate consistent user profile for this journey
        user_id = str(uuid.uuid4())
        customer_id = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        order_id = str(uuid.uuid4()) if random.random() < 0.7 else None  # 70% have order_id
        
        customer_segment = random.choice(customer_segments)
        order_frequency_tier = random.choice(order_frequency_tiers)
        spending_tier = random.choice(spending_tiers)
        preferred_cuisine = random.choice(cuisine_types)
        dietary_preference = random.choice(dietary_preferences)
        
        # Technical details
        platform = random.choice(platforms)
        device_model = random.choice(device_models)
        os = random.choice(operating_systems)
        app_version = f"{random.randint(8, 12)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        user_agent = f"FoodDeliveryApp/{app_version} ({platform}; {os})"
        
        # Geographic data
        country = 'United States'
        state = fake.state()
        city = fake.city()
        zip_code = fake.zipcode()
        delivery_zone = f"Zone_{random.randint(1, 15)}"
        
        # Restaurant and food details
        restaurant_name = random.choice(restaurant_names)
        restaurant_category = random.choice(restaurant_categories)
        cuisine_type = random.choice(cuisine_types)
        restaurant_rating = round(random.uniform(3.0, 5.0), 2)
        delivery_time_estimate = random.randint(15, 60)
        
        # Order details
        payment_method = random.choice(payment_methods)
        delivery_address_type = random.choice(delivery_address_types)
        
        # Contextual data
        is_first_order = order_frequency_tier == 'first_time'
        hour = random.randint(0, 23)
        is_peak_hours = hour in [11, 12, 13, 17, 18, 19, 20]  # Lunch and dinner peaks
        weather_condition = random.choice(weather_conditions)
        
        # Choose a journey template
        journey_name = random.choice(list(journey_templates.keys()))
        journey_template = journey_templates[journey_name]
        
        # Build the actual event sequence from the template
        event_sequence = []
        for event_category, count in journey_template['base_flow']:
            selected_events = random.sample(shared_events[event_category], min(count, len(shared_events[event_category])))
            event_sequence.extend(selected_events)
        
        # Add some randomization - 20% chance to add extra events
        if random.random() < 0.20:
            extra_categories = [cat for cat in shared_events.keys() if cat not in ['session_end']]
            extra_category = random.choice(extra_categories)
            extra_event = random.choice(shared_events[extra_category])
            insert_pos = random.randint(1, len(event_sequence) - 1)
            event_sequence.insert(insert_pos, extra_event)
        
        # Generate journey start time
        journey_start = datetime.now() - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Generate events for the journey
        converted = False
        total_order_value = 0
        
        for i, event_type in enumerate(event_sequence):
            # Calculate event timestamp
            if i == 0:
                event_timestamp = journey_start
            else:
                # Food delivery events have varied timing
                if event_details.get(event_type, {}).get('category') == 'tracking':
                    gap_seconds = random.randint(300, 1800)  # 5-30 minutes for tracking events
                elif event_details.get(event_type, {}).get('category') == 'marketing':
                    gap_seconds = random.randint(0, 60)  # Quick response to marketing
                else:
                    gap_seconds = random.randint(15, 300)  # 15 seconds to 5 minutes
                
                event_timestamp = previous_timestamp + timedelta(seconds=gap_seconds)
            
            previous_timestamp = event_timestamp
            
            # Get event information
            event_info = event_details.get(event_type, {
                'name': event_type.replace('_', ' ').title(),
                'url': f'/{event_type.replace("_", "/")}',
                'type': 'general',
                'category': 'other'
            })
            
            # Item and pricing details
            item_name = None
            item_category = None
            item_price = None
            
            if event_info['category'] in ['restaurant', 'menu', 'cart']:
                item_names = [
                    'Margherita Pizza', 'Cheeseburger Deluxe', 'Chicken Tacos', 'California Roll',
                    'Pad Thai', 'BBQ Ribs', 'Caesar Salad', 'Butter Chicken', 'Carbonara Pasta',
                    'Buffalo Wings', 'Tom Yum Soup', 'Gyro Platter', 'Club Sandwich',
                    'Chocolate Cake', 'Iced Coffee', 'Pancake Stack', 'Fish & Chips'
                ]
                item_name = random.choice(item_names)
                item_category = random.choice(item_categories)
                item_price = round(random.uniform(8, 35), 2)
            
            # Order calculations
            order_subtotal = None
            delivery_fee = None
            service_fee = None
            tip_amount = None
            taxes = None
            total_order_value = None
            discount_amount = None
            promo_code_used = None
            estimated_delivery_time = None
            actual_delivery_time = None
            delivery_instructions = None
            driver_rating = None
            revenue_impact = None
            
            # Marketing fields
            campaign_id = None
            notification_type = None
            email_campaign_name = None
            
            if event_info['category'] == 'marketing':
                campaign_id = str(uuid.uuid4())
                notification_type = random.choice(notification_types)
                email_campaign_name = random.choice([
                    'Weekend Special', 'Lunch Deal Alert', 'New Restaurant Launch', 
                    'Flash Sale', 'Loyalty Rewards', 'Weather-Based Offer'
                ])
            
            # Order-related calculations
            if event_info['category'] == 'checkout':
                order_subtotal = round(random.uniform(15, 80), 2)
                delivery_fee = round(random.uniform(1.99, 5.99), 2)
                service_fee = round(order_subtotal * 0.15, 2)  # 15% service fee
                tip_amount = round(order_subtotal * random.uniform(0.15, 0.25), 2)
                taxes = round(order_subtotal * 0.08875, 2)  # ~8.875% tax
                
                if random.random() < 0.3:  # 30% use promo codes
                    promo_codes = ['SAVE20', 'FREEDELIV', 'NEWUSER15', 'FLASH30', 'WELCOME10']
                    promo_code_used = random.choice(promo_codes)
                    discount_amount = round(order_subtotal * random.uniform(0.10, 0.30), 2)
                
                total_order_value = order_subtotal + delivery_fee + service_fee + tip_amount + taxes
                if discount_amount:
                    total_order_value -= discount_amount
                
                total_order_value = round(total_order_value, 2)
            
            if event_info['category'] == 'tracking':
                estimated_delivery_time = random.randint(25, 60)
                actual_delivery_time = estimated_delivery_time + random.randint(-10, 20)
                delivery_instructions = random.choice([
                    'Leave at door', 'Ring doorbell', 'Call when arrived', 
                    'Meet at lobby', 'Contactless delivery', None
                ])
            
            if event_type == 'driver_rating':
                driver_rating = round(random.uniform(3.0, 5.0), 2)
            
            # Conversion determination
            conversion_events = ['order_placed_success', 'order_delivered']
            is_conversion_event = event_type in conversion_events
            
            if is_conversion_event and random.random() < journey_template['conversion_rate']:
                converted = True
                if journey_template['revenue_range'][1] > 0:
                    revenue_impact = round(random.uniform(*journey_template['revenue_range']), 2)
            
            # Custom food delivery events (counts)
            restaurant_views = 1 if event_info['category'] in ['discovery', 'restaurant'] else 0
            menu_item_views = 1 if event_info['category'] == 'menu' else 0
            cart_additions = 1 if event_type == 'add_to_cart' else 0
            order_placements = 1 if event_type == 'order_placed_success' else 0
            reorders = 1 if event_type == 'add_to_cart' and order_frequency_tier != 'first_time' else 0
            
            yield (
                # Core identifiers
                str(uuid.uuid4()),  # event_id
                user_id,  # user_id (consistent)
                customer_id,  # customer_id (consistent)
                session_id,  # session_id (consistent)
                order_id,  # order_id
                
                # Event details
                event_timestamp,  # event_timestamp
                event_type,  # event_type
                event_info['category'],  # event_category
                event_type.replace('_', ' ').title(),  # event_action
                f"{journey_template['primary_goal']}_{event_type}",  # event_label
                
                # App/Platform information
                platform,  # platform
                app_version,  # app_version
                device_model,  # device_model
                os,  # operating_system
                user_agent,  # user_agent
                
                # Geographic data
                country,  # country
                state,  # state
                city,  # city
                zip_code,  # zip_code
                delivery_zone,  # delivery_zone
                
                # Restaurant and food data
                restaurant_name if event_info['category'] in ['restaurant', 'menu', 'cart', 'checkout'] else None,  # restaurant_name
                restaurant_category if event_info['category'] in ['restaurant', 'menu', 'cart', 'checkout'] else None,  # restaurant_category
                cuisine_type if event_info['category'] in ['restaurant', 'menu', 'cart', 'checkout'] else None,  # cuisine_type
                restaurant_rating if event_info['category'] in ['restaurant', 'feedback'] else None,  # restaurant_rating
                delivery_time_estimate if event_info['category'] in ['restaurant', 'checkout'] else None,  # delivery_time_estimate
                item_name,  # item_name
                item_category,  # item_category
                item_price,  # item_price
                
                # Order details
                order_subtotal,  # order_subtotal
                delivery_fee,  # delivery_fee
                service_fee,  # service_fee
                tip_amount,  # tip_amount
                taxes,  # taxes
                total_order_value,  # total_order_value
                payment_method if event_info['category'] == 'checkout' else None,  # payment_method
                
                # Delivery information
                delivery_address_type if event_info['category'] in ['checkout', 'tracking'] else None,  # delivery_address_type
                estimated_delivery_time,  # estimated_delivery_time
                actual_delivery_time,  # actual_delivery_time
                delivery_instructions,  # delivery_instructions
                driver_rating,  # driver_rating
                
                # Marketing and engagement
                campaign_id,  # campaign_id
                promo_code_used,  # promo_code_used
                discount_amount,  # discount_amount
                notification_type,  # notification_type
                email_campaign_name,  # email_campaign_name
                
                # Customer behavior dimensions
                customer_segment,  # customer_segment
                order_frequency_tier,  # order_frequency_tier
                spending_tier,  # spending_tier
                preferred_cuisine,  # preferred_cuisine
                dietary_preference,  # dietary_preferences
                
                # Custom food delivery events
                restaurant_views,  # restaurant_views
                menu_item_views,  # menu_item_views
                cart_additions,  # cart_additions
                order_placements,  # order_placements
                reorders,  # reorders
                
                # Additional context
                is_first_order,  # is_first_order
                is_peak_hours,  # is_peak_hours
                weather_condition,  # weather_condition
                is_conversion_event and converted,  # conversion_flag
                revenue_impact  # revenue_impact
            )
$$;

CREATE OR REPLACE TABLE delivery_events AS
SELECT e.*
FROM TABLE(GENERATOR(ROWCOUNT => $JOURNEY_COUNT)) g
CROSS JOIN TABLE(generate_food_delivery_journey()) e;

GRANT SELECT ON ALL TABLES IN SCHEMA SEQUENT_DB.DELIVERY TO ROLE SEQUENT_ROLE;

-- ===========================================================================
-- SECTION 8: GENERATE HOSPITALITY DATA
-- ===========================================================================

USE SCHEMA SEQUENT_DB.HOSPITALITY;

CREATE OR REPLACE FUNCTION generate_hotel_journey()
RETURNS TABLE (
    -- Core identifiers
    event_id STRING,
    visitor_id STRING,
    customer_id STRING,
    
    -- Event details
    event_timestamp TIMESTAMP,
    event_type STRING,
    event_category STRING,
    event_action STRING,
    event_label STRING,
    
    -- Page/Screen information (Adobe Analytics style)
    page_name STRING,
    page_url STRING,
    page_type STRING,
    site_section STRING,
    referrer_url STRING,
    
    -- Technical details
    browser STRING,
    browser_version STRING,
    operating_system STRING,
    device_type STRING,
    screen_resolution STRING,
    user_agent STRING,
    ip_address STRING,
    
    -- Geographic data
    country STRING,
    state STRING,
    city STRING,
    zip_code STRING,
    
    -- Page interaction details
    time_on_page INT,
    scroll_depth INT,
    clicks_on_page INT,
    
    -- Hotel/Travel specific fields
    hotel_name STRING,
    hotel_brand STRING,
    destination_city STRING,
    destination_country STRING,
    room_type STRING,
    rate_plan STRING,
    check_in_date DATE,
    check_out_date DATE,
    nights_stay INT,
    guests_count INT,
    room_rate DECIMAL(10,2),
    total_booking_value DECIMAL(12,2),
    currency_code STRING,
    
    -- Campaign/Marketing (Adobe Analytics style)
    campaign_id STRING,
    traffic_source STRING,
    medium STRING,
    referrer_domain STRING,
    
    -- Custom dimensions with explicit names
    traveler_type STRING,
    booking_purpose STRING,
    loyalty_tier STRING,
    advance_booking_days INT,
    price_sensitivity STRING,
    
    -- Custom events with explicit names
    hotel_searches INT,
    property_views INT,
    booking_starts INT,
    booking_completions INT,
    cancellation_requests INT,
    
    -- Additional context
    is_mobile_app BOOLEAN,
    page_load_time_ms INT,
    conversion_flag BOOLEAN,
    revenue_impact DECIMAL(12,2)
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'generateJourney'
PACKAGES = ('faker')
AS $$
import random
import uuid
from datetime import datetime, timedelta, date
from faker import Faker

fake = Faker()

class generateJourney:
    def process(self):
        # Define shared event pools for hotel/travel booking
        shared_events = {
            'entry_points': [
                'homepage_visit', 'destination_landing', 'search_result_click', 'email_campaign_click',
                'social_media_click', 'mobile_app_open', 'travel_blog_referral', 'google_ads_click',
                'ota_comparison_click', 'direct_url_entry', 'metasearch_referral'
            ],
            'authentication': [
                'login_attempt', 'login_success', 'guest_booking_start', 'account_creation_start',
                'password_reset_request', 'social_login_attempt', 'loyalty_login', 'corporate_login'
            ],
            'search_discovery': [
                'destination_search', 'date_selection', 'guest_count_selection', 'search_execution',
                'search_refinement', 'filter_application', 'sort_by_price', 'sort_by_rating',
                'map_view_toggle', 'list_view_toggle', 'availability_check'
            ],
            'property_research': [
                'hotel_detail_view', 'photo_gallery_view', 'amenities_section_view', 'location_map_view',
                'reviews_section_view', 'room_types_comparison', 'rate_calendar_view', 'virtual_tour_start',
                'nearby_attractions_view', 'hotel_policies_view', 'cancellation_policy_view'
            ],
            'booking_process': [
                'room_selection', 'rate_plan_selection', 'guest_info_entry', 'special_requests_entry',
                'extras_selection', 'payment_info_entry', 'booking_review', 'terms_acceptance',
                'booking_confirmation', 'confirmation_email_sent'
            ],
            'account_management': [
                'my_trips_view', 'booking_history_view', 'profile_update', 'preferences_update',
                'payment_methods_management', 'loyalty_account_view', 'points_balance_check',
                'membership_benefits_view', 'communication_preferences'
            ],
            'trip_planning': [
                'itinerary_builder', 'destination_guide_view', 'weather_check', 'flight_search',
                'car_rental_search', 'activity_booking', 'restaurant_reservations', 'travel_insurance_view',
                'packing_list_creation', 'travel_tips_view'
            ],
            'loyalty_rewards': [
                'loyalty_program_join', 'points_earning_view', 'points_redemption', 'tier_benefits_view',
                'elite_status_check', 'bonus_points_offers', 'partner_offers_view', 'reward_nights_booking'
            ],
            'reviews_feedback': [
                'review_submission', 'review_reading', 'rating_submission', 'photo_review_upload',
                'experience_sharing', 'recommendation_writing', 'complaint_submission', 'feedback_survey'
            ],
            'support_touchpoints': [
                'help_center_visit', 'faq_browse', 'live_chat_initiate', 'phone_support_request',
                'booking_modification_request', 'cancellation_request', 'refund_inquiry',
                'special_assistance_request', 'group_booking_inquiry', 'concierge_service_request'
            ],
            'mobile_specific': [
                'mobile_check_in', 'digital_key_setup', 'room_service_order', 'housekeeping_request',
                'wake_up_call_setup', 'mobile_checkout', 'push_notification_interaction',
                'location_services_enable', 'offline_itinerary_access'
            ],
            'promotional': [
                'deal_alerts_signup', 'flash_sale_participation', 'package_deal_view', 'group_discount_inquiry',
                'corporate_rate_access', 'promo_code_entry', 'last_minute_deals_view', 'seasonal_promotion_view',
                'loyalty_bonus_activation', 'referral_program_use'
            ],
            'comparison_shopping': [
                'rate_comparison_view', 'amenities_comparison', 'location_comparison', 'review_score_comparison',
                'price_alert_setup', 'competitor_rate_check', 'value_for_money_analysis', 'alternative_dates_check'
            ],
            'exits': [
                'logout', 'session_timeout', 'navigation_away', 'app_background',
                'browser_close', 'booking_abandonment', 'search_abandonment'
            ]
        }
        
        # Define journey templates for hotel booking
        journey_templates = {
            'leisure_vacation_booking': {
                'primary_goal': 'vacation_booking',
                'base_flow': [
                    ('entry_points', 1),
                    ('search_discovery', random.randint(2, 4)),
                    ('property_research', random.randint(3, 6)),
                    ('comparison_shopping', random.randint(1, 3)),
                    ('trip_planning', random.randint(0, 2)),
                    ('booking_process', random.randint(4, 8)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.18,
                'revenue_range': (200, 1500)
            },
            'business_travel_booking': {
                'primary_goal': 'business_booking',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', 1),
                    ('search_discovery', random.randint(1, 3)),
                    ('property_research', random.randint(1, 3)),
                    ('booking_process', random.randint(3, 6)),
                    ('account_management', random.randint(0, 1)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.35,
                'revenue_range': (150, 800)
            },
            'last_minute_booking': {
                'primary_goal': 'urgent_booking',
                'base_flow': [
                    ('entry_points', 1),
                    ('promotional', random.randint(1, 2)),
                    ('search_discovery', random.randint(1, 2)),
                    ('property_research', random.randint(1, 3)),
                    ('booking_process', random.randint(3, 5)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.45,
                'revenue_range': (100, 600)
            },
            'research_planning_phase': {
                'primary_goal': 'travel_research',
                'base_flow': [
                    ('entry_points', 1),
                    ('search_discovery', random.randint(3, 6)),
                    ('property_research', random.randint(4, 8)),
                    ('trip_planning', random.randint(2, 4)),
                    ('comparison_shopping', random.randint(2, 4)),
                    ('support_touchpoints', random.randint(0, 2)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.05,
                'revenue_range': (0, 0)
            },
            'loyalty_member_booking': {
                'primary_goal': 'loyalty_booking',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', 1),
                    ('loyalty_rewards', random.randint(1, 3)),
                    ('search_discovery', random.randint(1, 3)),
                    ('property_research', random.randint(2, 4)),
                    ('booking_process', random.randint(3, 6)),
                    ('account_management', random.randint(0, 1)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.42,
                'revenue_range': (180, 1200)
            },
            'group_event_booking': {
                'primary_goal': 'group_booking',
                'base_flow': [
                    ('entry_points', 1),
                    ('search_discovery', random.randint(2, 4)),
                    ('property_research', random.randint(3, 5)),
                    ('support_touchpoints', random.randint(2, 4)),
                    ('comparison_shopping', random.randint(1, 3)),
                    ('booking_process', random.randint(4, 7)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.25,
                'revenue_range': (800, 5000)
            },
            'mobile_app_browsing': {
                'primary_goal': 'mobile_engagement',
                'base_flow': [
                    ('entry_points', 1),
                    ('mobile_specific', random.randint(2, 4)),
                    ('search_discovery', random.randint(2, 4)),
                    ('property_research', random.randint(1, 3)),
                    ('booking_process', random.randint(0, 4)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.15,
                'revenue_range': (120, 700)
            },
            'customer_service_interaction': {
                'primary_goal': 'service_request',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', random.randint(0, 1)),
                    ('account_management', random.randint(1, 2)),
                    ('support_touchpoints', random.randint(3, 6)),
                    ('booking_process', random.randint(0, 3)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.30,
                'revenue_range': (0, 200)
            },
            'booking_modification': {
                'primary_goal': 'change_booking',
                'base_flow': [
                    ('entry_points', 1),
                    ('authentication', 1),
                    ('account_management', random.randint(1, 2)),
                    ('support_touchpoints', random.randint(1, 3)),
                    ('search_discovery', random.randint(0, 2)),
                    ('booking_process', random.randint(0, 4)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.55,
                'revenue_range': (-100, 300)
            },
            'price_comparison_shopping': {
                'primary_goal': 'price_research',
                'base_flow': [
                    ('entry_points', 1),
                    ('search_discovery', random.randint(2, 4)),
                    ('comparison_shopping', random.randint(3, 6)),
                    ('property_research', random.randint(2, 5)),
                    ('promotional', random.randint(1, 2)),
                    ('booking_process', random.randint(0, 3)),
                    ('exits', 1)
                ],
                'conversion_rate': 0.12,
                'revenue_range': (100, 800)
            }
        }
        
        # Detailed event mappings for hotel/travel booking
        event_details = {
            # Entry points
            'homepage_visit': {'name': 'Homepage', 'url': '/', 'type': 'marketing', 'category': 'navigation'},
            'destination_landing': {'name': 'Destination Landing', 'url': '/destinations/paris', 'type': 'marketing', 'category': 'destination'},
            'search_result_click': {'name': 'Search Results', 'url': '/search-results', 'type': 'search', 'category': 'search'},
            'email_campaign_click': {'name': 'Email Campaign', 'url': '/campaign/summer-deals', 'type': 'marketing', 'category': 'campaign'},
            'social_media_click': {'name': 'Social Media', 'url': '/social-landing', 'type': 'marketing', 'category': 'social'},
            'mobile_app_open': {'name': 'Mobile App Home', 'url': '/app/home', 'type': 'mobile', 'category': 'mobile'},
            'travel_blog_referral': {'name': 'Travel Blog', 'url': '/blog-referral', 'type': 'marketing', 'category': 'content'},
            'google_ads_click': {'name': 'Google Ads', 'url': '/ads-landing', 'type': 'marketing', 'category': 'paid_search'},
            'ota_comparison_click': {'name': 'OTA Comparison', 'url': '/ota-referral', 'type': 'marketing', 'category': 'comparison'},
            'direct_url_entry': {'name': 'Direct Entry', 'url': '/direct', 'type': 'direct', 'category': 'direct'},
            'metasearch_referral': {'name': 'Metasearch', 'url': '/metasearch-referral', 'type': 'marketing', 'category': 'metasearch'},
            
            # Authentication
            'login_attempt': {'name': 'Login Page', 'url': '/login', 'type': 'authentication', 'category': 'auth'},
            'login_success': {'name': 'Login Success', 'url': '/my-account', 'type': 'authentication', 'category': 'auth'},
            'guest_booking_start': {'name': 'Guest Booking', 'url': '/book-as-guest', 'type': 'booking', 'category': 'auth'},
            'account_creation_start': {'name': 'Create Account', 'url': '/register', 'type': 'authentication', 'category': 'auth'},
            'password_reset_request': {'name': 'Password Reset', 'url': '/forgot-password', 'type': 'authentication', 'category': 'auth'},
            'social_login_attempt': {'name': 'Social Login', 'url': '/login/social', 'type': 'authentication', 'category': 'auth'},
            'loyalty_login': {'name': 'Loyalty Login', 'url': '/loyalty/login', 'type': 'authentication', 'category': 'loyalty'},
            'corporate_login': {'name': 'Corporate Login', 'url': '/corporate/login', 'type': 'authentication', 'category': 'corporate'},
            
            # Search & Discovery
            'destination_search': {'name': 'Destination Search', 'url': '/search', 'type': 'search', 'category': 'search'},
            'date_selection': {'name': 'Date Selection', 'url': '/search/dates', 'type': 'search', 'category': 'search'},
            'guest_count_selection': {'name': 'Guest Count', 'url': '/search/guests', 'type': 'search', 'category': 'search'},
            'search_execution': {'name': 'Execute Search', 'url': '/search/results', 'type': 'search', 'category': 'search'},
            'search_refinement': {'name': 'Refine Search', 'url': '/search/refine', 'type': 'search', 'category': 'search'},
            'filter_application': {'name': 'Apply Filters', 'url': '/search/filters', 'type': 'search', 'category': 'filter'},
            'sort_by_price': {'name': 'Sort by Price', 'url': '/search/sort-price', 'type': 'search', 'category': 'sort'},
            'sort_by_rating': {'name': 'Sort by Rating', 'url': '/search/sort-rating', 'type': 'search', 'category': 'sort'},
            'map_view_toggle': {'name': 'Map View', 'url': '/search/map', 'type': 'search', 'category': 'view'},
            'list_view_toggle': {'name': 'List View', 'url': '/search/list', 'type': 'search', 'category': 'view'},
            'availability_check': {'name': 'Check Availability', 'url': '/search/availability', 'type': 'search', 'category': 'availability'},
            
            # Property Research
            'hotel_detail_view': {'name': 'Hotel Details', 'url': '/hotel/grand-plaza-paris', 'type': 'property', 'category': 'property'},
            'photo_gallery_view': {'name': 'Photo Gallery', 'url': '/hotel/grand-plaza-paris/photos', 'type': 'property', 'category': 'media'},
            'amenities_section_view': {'name': 'Hotel Amenities', 'url': '/hotel/grand-plaza-paris/amenities', 'type': 'property', 'category': 'amenities'},
            'location_map_view': {'name': 'Location Map', 'url': '/hotel/grand-plaza-paris/location', 'type': 'property', 'category': 'location'},
            'reviews_section_view': {'name': 'Guest Reviews', 'url': '/hotel/grand-plaza-paris/reviews', 'type': 'property', 'category': 'reviews'},
            'room_types_comparison': {'name': 'Room Types', 'url': '/hotel/grand-plaza-paris/rooms', 'type': 'property', 'category': 'rooms'},
            'rate_calendar_view': {'name': 'Rate Calendar', 'url': '/hotel/grand-plaza-paris/rates', 'type': 'property', 'category': 'pricing'},
            'virtual_tour_start': {'name': 'Virtual Tour', 'url': '/hotel/grand-plaza-paris/tour', 'type': 'property', 'category': 'media'},
            'nearby_attractions_view': {'name': 'Nearby Attractions', 'url': '/hotel/grand-plaza-paris/attractions', 'type': 'property', 'category': 'location'},
            'hotel_policies_view': {'name': 'Hotel Policies', 'url': '/hotel/grand-plaza-paris/policies', 'type': 'property', 'category': 'policies'},
            'cancellation_policy_view': {'name': 'Cancellation Policy', 'url': '/hotel/grand-plaza-paris/cancellation', 'type': 'property', 'category': 'policies'},
            
            # Booking Process
            'room_selection': {'name': 'Select Room', 'url': '/book/room-selection', 'type': 'booking', 'category': 'booking'},
            'rate_plan_selection': {'name': 'Select Rate Plan', 'url': '/book/rate-plan', 'type': 'booking', 'category': 'booking'},
            'guest_info_entry': {'name': 'Guest Information', 'url': '/book/guest-info', 'type': 'booking', 'category': 'booking'},
            'special_requests_entry': {'name': 'Special Requests', 'url': '/book/special-requests', 'type': 'booking', 'category': 'booking'},
            'extras_selection': {'name': 'Select Extras', 'url': '/book/extras', 'type': 'booking', 'category': 'booking'},
            'payment_info_entry': {'name': 'Payment Information', 'url': '/book/payment', 'type': 'booking', 'category': 'payment'},
            'booking_review': {'name': 'Review Booking', 'url': '/book/review', 'type': 'booking', 'category': 'booking'},
            'terms_acceptance': {'name': 'Accept Terms', 'url': '/book/terms', 'type': 'booking', 'category': 'legal'},
            'booking_confirmation': {'name': 'Booking Confirmed', 'url': '/book/confirmation', 'type': 'booking', 'category': 'confirmation'},
            'confirmation_email_sent': {'name': 'Confirmation Email', 'url': '/email/confirmation', 'type': 'email', 'category': 'confirmation'},
            
            # Account Management
            'my_trips_view': {'name': 'My Trips', 'url': '/my-account/trips', 'type': 'account', 'category': 'account'},
            'booking_history_view': {'name': 'Booking History', 'url': '/my-account/history', 'type': 'account', 'category': 'account'},
            'profile_update': {'name': 'Update Profile', 'url': '/my-account/profile', 'type': 'account', 'category': 'account'},
            'preferences_update': {'name': 'Travel Preferences', 'url': '/my-account/preferences', 'type': 'account', 'category': 'preferences'},
            'payment_methods_management': {'name': 'Payment Methods', 'url': '/my-account/payments', 'type': 'account', 'category': 'payment'},
            'loyalty_account_view': {'name': 'Loyalty Account', 'url': '/loyalty/account', 'type': 'account', 'category': 'loyalty'},
            'points_balance_check': {'name': 'Points Balance', 'url': '/loyalty/points', 'type': 'account', 'category': 'loyalty'},
            'membership_benefits_view': {'name': 'Member Benefits', 'url': '/loyalty/benefits', 'type': 'account', 'category': 'loyalty'},
            'communication_preferences': {'name': 'Communication Prefs', 'url': '/my-account/communications', 'type': 'account', 'category': 'preferences'},
            
            # Trip Planning
            'itinerary_builder': {'name': 'Itinerary Builder', 'url': '/trip-planner', 'type': 'planning', 'category': 'planning'},
            'destination_guide_view': {'name': 'Destination Guide', 'url': '/guides/paris', 'type': 'planning', 'category': 'destination'},
            'weather_check': {'name': 'Weather Forecast', 'url': '/weather/paris', 'type': 'planning', 'category': 'weather'},
            'flight_search': {'name': 'Flight Search', 'url': '/flights', 'type': 'planning', 'category': 'flights'},
            'car_rental_search': {'name': 'Car Rental', 'url': '/cars', 'type': 'planning', 'category': 'transport'},
            'activity_booking': {'name': 'Activity Booking', 'url': '/activities', 'type': 'planning', 'category': 'activities'},
            'restaurant_reservations': {'name': 'Restaurant Reservations', 'url': '/restaurants', 'type': 'planning', 'category': 'dining'},
            'travel_insurance_view': {'name': 'Travel Insurance', 'url': '/insurance', 'type': 'planning', 'category': 'insurance'},
            'packing_list_creation': {'name': 'Packing List', 'url': '/packing-list', 'type': 'planning', 'category': 'preparation'},
            'travel_tips_view': {'name': 'Travel Tips', 'url': '/tips', 'type': 'planning', 'category': 'tips'},
            
            # Loyalty & Rewards
            'loyalty_program_join': {'name': 'Join Loyalty Program', 'url': '/loyalty/join', 'type': 'loyalty', 'category': 'loyalty'},
            'points_earning_view': {'name': 'Earn Points', 'url': '/loyalty/earn', 'type': 'loyalty', 'category': 'loyalty'},
            'points_redemption': {'name': 'Redeem Points', 'url': '/loyalty/redeem', 'type': 'loyalty', 'category': 'redemption'},
            'tier_benefits_view': {'name': 'Tier Benefits', 'url': '/loyalty/tiers', 'type': 'loyalty', 'category': 'benefits'},
            'elite_status_check': {'name': 'Elite Status', 'url': '/loyalty/elite', 'type': 'loyalty', 'category': 'status'},
            'bonus_points_offers': {'name': 'Bonus Points Offers', 'url': '/loyalty/bonus', 'type': 'loyalty', 'category': 'offers'},
            'partner_offers_view': {'name': 'Partner Offers', 'url': '/loyalty/partners', 'type': 'loyalty', 'category': 'partners'},
            'reward_nights_booking': {'name': 'Reward Nights', 'url': '/loyalty/reward-nights', 'type': 'loyalty', 'category': 'rewards'},
            
            # Reviews & Feedback
            'review_submission': {'name': 'Submit Review', 'url': '/review/submit', 'type': 'social', 'category': 'review'},
            'review_reading': {'name': 'Read Reviews', 'url': '/reviews', 'type': 'social', 'category': 'social_proof'},
            'rating_submission': {'name': 'Submit Rating', 'url': '/rating/submit', 'type': 'social', 'category': 'rating'},
            'photo_review_upload': {'name': 'Photo Review', 'url': '/review/photos', 'type': 'social', 'category': 'ugc'},
            'experience_sharing': {'name': 'Share Experience', 'url': '/share-experience', 'type': 'social', 'category': 'sharing'},
            'recommendation_writing': {'name': 'Write Recommendation', 'url': '/recommend', 'type': 'social', 'category': 'recommendation'},
            'complaint_submission': {'name': 'Submit Complaint', 'url': '/complaint', 'type': 'support', 'category': 'complaint'},
            'feedback_survey': {'name': 'Feedback Survey', 'url': '/survey', 'type': 'feedback', 'category': 'survey'},
            
            # Support Touchpoints
            'help_center_visit': {'name': 'Help Center', 'url': '/help', 'type': 'support', 'category': 'support'},
            'faq_browse': {'name': 'FAQ', 'url': '/faq', 'type': 'support', 'category': 'support'},
            'live_chat_initiate': {'name': 'Live Chat', 'url': '/chat', 'type': 'support', 'category': 'support'},
            'phone_support_request': {'name': 'Phone Support', 'url': '/support/phone', 'type': 'support', 'category': 'phone'},
            'booking_modification_request': {'name': 'Modify Booking', 'url': '/modify-booking', 'type': 'support', 'category': 'modification'},
            'cancellation_request': {'name': 'Cancel Booking', 'url': '/cancel-booking', 'type': 'support', 'category': 'cancellation'},
            'refund_inquiry': {'name': 'Refund Inquiry', 'url': '/refund', 'type': 'support', 'category': 'refund'},
            'special_assistance_request': {'name': 'Special Assistance', 'url': '/special-assistance', 'type': 'support', 'category': 'assistance'},
            'group_booking_inquiry': {'name': 'Group Booking', 'url': '/group-booking', 'type': 'support', 'category': 'group'},
            'concierge_service_request': {'name': 'Concierge Service', 'url': '/concierge', 'type': 'support', 'category': 'concierge'},
            
            # Mobile Specific
            'mobile_check_in': {'name': 'Mobile Check-in', 'url': '/app/check-in', 'type': 'mobile', 'category': 'checkin'},
            'digital_key_setup': {'name': 'Digital Key', 'url': '/app/digital-key', 'type': 'mobile', 'category': 'key'},
            'room_service_order': {'name': 'Room Service', 'url': '/app/room-service', 'type': 'mobile', 'category': 'service'},
            'housekeeping_request': {'name': 'Housekeeping Request', 'url': '/app/housekeeping', 'type': 'mobile', 'category': 'service'},
            'wake_up_call_setup': {'name': 'Wake-up Call', 'url': '/app/wake-up', 'type': 'mobile', 'category': 'service'},
            'mobile_checkout': {'name': 'Mobile Checkout', 'url': '/app/checkout', 'type': 'mobile', 'category': 'checkout'},
            'push_notification_interaction': {'name': 'Push Notification', 'url': '/app/notification', 'type': 'mobile', 'category': 'notification'},
            'location_services_enable': {'name': 'Location Services', 'url': '/app/location', 'type': 'mobile', 'category': 'location'},
            'offline_itinerary_access': {'name': 'Offline Itinerary', 'url': '/app/offline', 'type': 'mobile', 'category': 'offline'},
            
            # Promotional
            'deal_alerts_signup': {'name': 'Deal Alerts', 'url': '/deals/alerts', 'type': 'promotion', 'category': 'alerts'},
            'flash_sale_participation': {'name': 'Flash Sale', 'url': '/flash-sale', 'type': 'promotion', 'category': 'flash_sale'},
            'package_deal_view': {'name': 'Package Deals', 'url': '/packages', 'type': 'promotion', 'category': 'packages'},
            'group_discount_inquiry': {'name': 'Group Discounts', 'url': '/group-discounts', 'type': 'promotion', 'category': 'group'},
            'corporate_rate_access': {'name': 'Corporate Rates', 'url': '/corporate-rates', 'type': 'promotion', 'category': 'corporate'},
            'promo_code_entry': {'name': 'Promo Code', 'url': '/promo-code', 'type': 'promotion', 'category': 'promo'},
            'last_minute_deals_view': {'name': 'Last Minute Deals', 'url': '/last-minute', 'type': 'promotion', 'category': 'last_minute'},
            'seasonal_promotion_view': {'name': 'Seasonal Promotion', 'url': '/seasonal-deals', 'type': 'promotion', 'category': 'seasonal'},
            'loyalty_bonus_activation': {'name': 'Loyalty Bonus', 'url': '/loyalty/bonus-activation', 'type': 'promotion', 'category': 'loyalty_bonus'},
            'referral_program_use': {'name': 'Referral Program', 'url': '/referral', 'type': 'promotion', 'category': 'referral'},
            
            # Comparison Shopping
            'rate_comparison_view': {'name': 'Rate Comparison', 'url': '/compare/rates', 'type': 'comparison', 'category': 'pricing'},
            'amenities_comparison': {'name': 'Amenities Comparison', 'url': '/compare/amenities', 'type': 'comparison', 'category': 'amenities'},
            'location_comparison': {'name': 'Location Comparison', 'url': '/compare/location', 'type': 'comparison', 'category': 'location'},
            'review_score_comparison': {'name': 'Review Comparison', 'url': '/compare/reviews', 'type': 'comparison', 'category': 'reviews'},
            'price_alert_setup': {'name': 'Price Alerts', 'url': '/price-alerts', 'type': 'comparison', 'category': 'alerts'},
            'competitor_rate_check': {'name': 'Competitor Rates', 'url': '/competitor-rates', 'type': 'comparison', 'category': 'competitive'},
            'value_for_money_analysis': {'name': 'Value Analysis', 'url': '/value-analysis', 'type': 'comparison', 'category': 'value'},
            'alternative_dates_check': {'name': 'Alternative Dates', 'url': '/alternative-dates', 'type': 'comparison', 'category': 'dates'},
            
            # Exits
            'logout': {'name': 'Logout', 'url': '/logout', 'type': 'authentication', 'category': 'exit'},
            'session_timeout': {'name': 'Session Timeout', 'url': '/timeout', 'type': 'system', 'category': 'exit'},
            'navigation_away': {'name': 'Navigate Away', 'url': '/external', 'type': 'system', 'category': 'exit'},
            'app_background': {'name': 'App Background', 'url': '/app/background', 'type': 'mobile', 'category': 'exit'},
            'browser_close': {'name': 'Browser Close', 'url': '/close', 'type': 'system', 'category': 'exit'},
            'booking_abandonment': {'name': 'Booking Abandonment', 'url': '/booking/abandon', 'type': 'booking', 'category': 'abandonment'},
            'search_abandonment': {'name': 'Search Abandonment', 'url': '/search/abandon', 'type': 'search', 'category': 'abandonment'}
        }
        
        # Hotel and travel data
        hotel_names = [
            'Grand Plaza Hotel', 'Marriott Downtown', 'Hilton Garden Inn', 'Holiday Inn Express',
            'Hyatt Regency', 'Sheraton Grand', 'Four Seasons Resort', 'Ritz-Carlton',
            'Hampton Inn & Suites', 'Courtyard by Marriott', 'DoubleTree by Hilton',
            'Embassy Suites', 'Westin Resort', 'W Hotel', 'Aloft Hotel',
            'Renaissance Hotel', 'JW Marriott', 'St. Regis Resort', 'Edition Hotel',
            'Waldorf Astoria', 'Conrad Hotel', 'InterContinental', 'Crowne Plaza',
            'Hotel Indigo', 'Kimpton Hotel'
        ]
        
        hotel_brands = [
            'Marriott', 'Hilton', 'Hyatt', 'IHG', 'Accor', 'Wyndham',
            'Choice Hotels', 'Best Western', 'Radisson', 'Independent'
        ]
        
        destinations = [
            ('New York', 'United States'), ('Paris', 'France'), ('London', 'United Kingdom'),
            ('Tokyo', 'Japan'), ('Dubai', 'UAE'), ('Los Angeles', 'United States'),
            ('Barcelona', 'Spain'), ('Rome', 'Italy'), ('Amsterdam', 'Netherlands'),
            ('Singapore', 'Singapore'), ('Sydney', 'Australia'), ('Miami', 'United States'),
            ('Las Vegas', 'United States'), ('Bangkok', 'Thailand'), ('Istanbul', 'Turkey'),
            ('Berlin', 'Germany'), ('Vienna', 'Austria'), ('Prague', 'Czech Republic')
        ]
        
        room_types = [
            'Standard King', 'Standard Queen', 'Deluxe King', 'Deluxe Queen',
            'Junior Suite', 'Executive Suite', 'Presidential Suite',
            'Standard Double', 'Superior King', 'Premium Queen'
        ]
        
        rate_plans = [
            'Best Available Rate', 'Advance Purchase', 'Stay Longer Save More',
            'Flexible Rate', 'Non-Refundable', 'Corporate Rate', 'AAA Rate',
            'Senior Rate', 'Government Rate', 'Package Deal'
        ]
        
        traveler_types = [
            'leisure_solo', 'leisure_couple', 'leisure_family', 'business_solo',
            'business_group', 'group_leisure', 'group_business', 'bleisure'
        ]
        
        booking_purposes = [
            'vacation', 'business_meeting', 'conference', 'wedding', 'family_visit',
            'romantic_getaway', 'adventure_travel', 'city_break', 'staycation', 'relocation'
        ]
        
        loyalty_tiers = ['None', 'Silver', 'Gold', 'Platinum', 'Diamond', 'Titanium']
        price_sensitivities = ['budget', 'value', 'mid_range', 'luxury', 'ultra_luxury']
        
        channels = ['web_desktop', 'web_mobile', 'mobile_app', 'tablet_app']
        
        # Technical configurations
        browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Mobile Safari', 'Chrome Mobile', 'Samsung Internet']
        browser_versions = ['120.0', '119.0', '118.0', '117.0', '116.0', '115.0']
        operating_systems = [
            'Windows 10', 'Windows 11', 'macOS 14', 'macOS 13', 'macOS 12',
            'iOS 17', 'iOS 16', 'iOS 15', 'Android 14', 'Android 13', 'Android 12'
        ]
        device_types = ['Desktop', 'Mobile', 'Tablet']
        screen_resolutions = [
            '1920x1080', '1366x768', '1440x900', '2560x1440', '3840x2160',
            '375x667', '414x896', '390x844', '428x926',  # iPhone
            '1024x768', '1366x1024', '2048x2732'  # iPad
        ]
        
        # Campaign sources
        traffic_sources = [
            'direct', 'google', 'facebook', 'instagram', 'email', 'referral', 'paid_search',
            'booking.com', 'expedia', 'tripadvisor', 'kayak', 'priceline', 'travel_blog'
        ]
        mediums = ['organic', 'cpc', 'email', 'social', 'referral', 'direct', 'display', 'video', 'ota', 'metasearch']
        
        # Generate consistent user profile for this journey
        visitor_id = str(uuid.uuid4())
        customer_id = str(uuid.uuid4())
        traveler_type = random.choice(traveler_types)
        booking_purpose = random.choice(booking_purposes)
        loyalty_tier = random.choice(loyalty_tiers)
        price_sensitivity = random.choice(price_sensitivities)
        
        # Consistent geographic data
        state = fake.state()
        city = fake.city()
        zip_code = fake.zipcode()
        ip_address = fake.ipv4()
        
        # Choose a journey template
        journey_name = random.choice(list(journey_templates.keys()))
        journey_template = journey_templates[journey_name]
        
        # Build the actual event sequence from the template
        event_sequence = []
        for event_category, count in journey_template['base_flow']:
            selected_events = random.sample(shared_events[event_category], min(count, len(shared_events[event_category])))
            event_sequence.extend(selected_events)
        
        # Add some randomization - 20% chance to add extra cross-category events
        if random.random() < 0.20:
            extra_categories = [cat for cat in shared_events.keys() if cat not in ['exits']]
            extra_category = random.choice(extra_categories)
            extra_event = random.choice(shared_events[extra_category])
            # Insert at random position (not at the end)
            insert_pos = random.randint(1, len(event_sequence) - 1)
            event_sequence.insert(insert_pos, extra_event)
        
        # Consistent technical details for the journey
        device = random.choice(device_types)
        is_mobile = device in ['Mobile', 'Tablet']
        
        # Choose browser based on device
        if device == 'Mobile':
            browser = random.choice(['Mobile Safari', 'Chrome Mobile', 'Samsung Internet'])
            if browser == 'Mobile Safari':
                os = random.choice(['iOS 17', 'iOS 16', 'iOS 15'])
            else:
                os = random.choice(['Android 14', 'Android 13', 'Android 12'])
        elif device == 'Tablet':
            browser = random.choice(['Safari', 'Chrome', 'Mobile Safari'])
            os = random.choice(['iOS 17', 'iOS 16', 'macOS 14']) if 'Safari' in browser else random.choice(['Android 14', 'Windows 11'])
        else:  # Desktop
            browser = random.choice(['Chrome', 'Safari', 'Firefox', 'Edge'])
            if browser == 'Safari':
                os = random.choice(['macOS 14', 'macOS 13', 'macOS 12'])
            else:
                os = random.choice(['Windows 11', 'Windows 10', 'macOS 14'])
        
        browser_version = random.choice(browser_versions)
        resolution = random.choice(screen_resolutions)
        user_agent = f"{browser}/{browser_version} ({os})"
        
        # Channel determination
        is_mobile_app = is_mobile and random.random() < 0.25
        if is_mobile_app:
            channel = 'mobile_app' if device == 'Mobile' else 'tablet_app'
        else:
            channel = f"web_{device.lower()}"
        
        # Campaign attribution (consistent for the journey)
        has_campaign = random.random() < 0.45
        campaign_id = str(uuid.uuid4()) if has_campaign else None
        traffic_source = random.choice(traffic_sources) if has_campaign else 'direct'
        medium = random.choice(mediums) if has_campaign else 'direct'
        referrer_domain = fake.domain_name() if traffic_source == 'referral' else None
        
        # Travel details (consistent for the journey)
        destination_city, destination_country = random.choice(destinations)
        hotel_name = random.choice(hotel_names)
        hotel_brand = random.choice(hotel_brands)
        room_type = random.choice(room_types)
        rate_plan = random.choice(rate_plans)
        
        # Generate realistic travel dates
        advance_days = random.randint(1, 120)  # 1 to 120 days in advance
        check_in_date = date.today() + timedelta(days=advance_days)
        nights_stay = random.randint(1, 14)  # 1 to 14 nights
        check_out_date = check_in_date + timedelta(days=nights_stay)
        guests_count = random.randint(1, 4)
        
        # Generate room rate based on destination and room type
        base_rates = {'Standard': 120, 'Deluxe': 180, 'Suite': 300}
        rate_multiplier = 1.0
        for rate_type in base_rates:
            if rate_type.lower() in room_type.lower():
                room_rate = base_rates[rate_type] * rate_multiplier
                break
        else:
            room_rate = 150
        
        room_rate = round(room_rate * random.uniform(0.7, 1.5), 2)  # Add variability
        
        # Generate journey start time
        journey_start = datetime.now() - timedelta(
            days=random.randint(0, 90),
            hours=random.randint(6, 23),
            minutes=random.randint(0, 59)
        )
        
        # Site section mapping
        site_section_mapping = {
            'marketing': 'Marketing & Deals',
            'authentication': 'Account & Login',
            'search': 'Search & Discovery',
            'property': 'Hotel Details',
            'booking': 'Booking & Reservations',
            'account': 'My Account',
            'planning': 'Trip Planning',
            'loyalty': 'Loyalty Program',
            'social': 'Reviews & Community',
            'support': 'Customer Support',
            'mobile': 'Mobile Experience',
            'comparison': 'Price Comparison'
        }
        
        # Generate events for the journey
        previous_url = None
        converted = False
        total_booking_value = 0
        
        for i, event_type in enumerate(event_sequence):
            # Calculate event timestamp with realistic gaps
            if i == 0:
                event_timestamp = journey_start
            else:
                # Variable time gaps based on event category
                prev_category = event_details.get(event_sequence[i-1], {}).get('category', '')
                curr_category = event_details.get(event_type, {}).get('category', '')
                
                if prev_category == curr_category:
                    gap_seconds = random.randint(15, 120)  # 15 seconds to 2 minutes for related actions
                elif curr_category == 'booking':
                    gap_seconds = random.randint(45, 300)  # Longer for booking steps
                else:
                    gap_seconds = random.randint(30, 600)  # 30 seconds to 10 minutes for category changes
                
                event_timestamp = previous_timestamp + timedelta(seconds=gap_seconds)
            
            previous_timestamp = event_timestamp
            
            # Get event information
            event_info = event_details.get(event_type, {
                'name': event_type.replace('_', ' ').title(),
                'url': f'/{event_type.replace("_", "-")}',
                'type': 'general',
                'category': 'other'
            })
            
            # Hotel details for relevant events
            event_hotel_name = hotel_name if event_info['category'] in ['property', 'booking', 'confirmation'] else None
            event_hotel_brand = hotel_brand if event_hotel_name else None
            event_destination_city = destination_city if event_info['category'] in ['search', 'property', 'booking'] else None
            event_destination_country = destination_country if event_destination_city else None
            event_room_type = room_type if event_info['category'] == 'booking' else None
            event_rate_plan = rate_plan if event_info['category'] == 'booking' else None
            event_room_rate = room_rate if event_info['category'] in ['property', 'booking'] else None
            
            # Booking details for booking events
            event_check_in = check_in_date if event_info['category'] in ['search', 'booking'] else None
            event_check_out = check_out_date if event_check_in else None
            event_nights = nights_stay if event_check_in else None
            event_guests = guests_count if event_check_in else None
            
            # Currency (simplified to USD for this example)
            currency_code = 'USD'
            
            # Booking value and revenue calculation
            total_booking_value = None
            revenue_impact = None
            
            # Determine if this is a conversion event
            conversion_events = ['booking_confirmation', 'confirmation_email_sent']
            is_conversion_event = event_type in conversion_events
            
            if is_conversion_event and random.random() < journey_template['conversion_rate']:
                converted = True
                if journey_template['revenue_range'][1] > 0:
                    total_booking_value = round(room_rate * nights_stay * random.uniform(0.8, 1.2), 2)
                    revenue_impact = total_booking_value
                elif journey_template['revenue_range'][0] < 0:  # Cancellation/modification
                    revenue_impact = journey_template['revenue_range'][0]
            
            # Custom events with explicit names (counts)
            hotel_searches = 1 if event_info['category'] == 'search' else 0
            property_views = 1 if event_info['category'] == 'property' else 0
            booking_starts = 1 if event_type in ['room_selection', 'guest_info_entry'] else 0
            booking_completions = 1 if event_type == 'booking_confirmation' else 0
            cancellation_requests = 1 if event_type == 'cancellation_request' else 0
            
            # Page interaction metrics
            time_on_page = random.randint(10, 900)  # 10 seconds to 15 minutes
            scroll_depth = random.randint(15, 100)  # Percentage
            clicks_on_page = random.randint(0, 30)
            page_load_time = random.randint(200, 5000)  # milliseconds
            
            yield (
                # Core identifiers
                str(uuid.uuid4()),  # event_id
                visitor_id,  # visitor_id (consistent)
                customer_id,  # customer_id (consistent)
                
                # Event details
                event_timestamp,  # event_timestamp
                event_type,  # event_type
                event_info['category'],  # event_category
                event_type.replace('_', ' ').title(),  # event_action
                f"{journey_template['primary_goal']}_{event_type}",  # event_label
                
                # Page information
                event_info['name'],  # page_name
                event_info['url'],  # page_url
                event_info['type'],  # page_type
                site_section_mapping.get(event_info['type'], 'Other'),  # site_section
                previous_url,  # referrer_url
                
                # Technical details (consistent for journey)
                browser,  # browser
                browser_version,  # browser_version
                os,  # operating_system
                device,  # device_type
                resolution,  # screen_resolution
                user_agent,  # user_agent
                ip_address,  # ip_address
                
                # Geographic data (consistent for journey)
                'United States',  # country
                state,  # state
                city,  # city
                zip_code,  # zip_code
                
                # Page interaction details
                time_on_page,  # time_on_page
                scroll_depth,  # scroll_depth
                clicks_on_page,  # clicks_on_page
                
                # Hotel/Travel specific fields
                event_hotel_name,  # hotel_name
                event_hotel_brand,  # hotel_brand
                event_destination_city,  # destination_city
                event_destination_country,  # destination_country
                event_room_type,  # room_type
                event_rate_plan,  # rate_plan
                event_check_in,  # check_in_date
                event_check_out,  # check_out_date
                event_nights,  # nights_stay
                event_guests,  # guests_count
                event_room_rate,  # room_rate
                total_booking_value,  # total_booking_value
                currency_code,  # currency_code
                
                # Campaign/Marketing (consistent for journey)
                campaign_id,  # campaign_id
                traffic_source,  # traffic_source
                medium,  # medium
                referrer_domain,  # referrer_domain
                
                # Custom dimensions with explicit names (consistent for journey)
                traveler_type,  # traveler_type
                booking_purpose,  # booking_purpose
                loyalty_tier,  # loyalty_tier
                advance_days,  # advance_booking_days
                price_sensitivity,  # price_sensitivity
                
                # Custom events with explicit names
                hotel_searches,  # hotel_searches
                property_views,  # property_views
                booking_starts,  # booking_starts
                booking_completions,  # booking_completions
                cancellation_requests,  # cancellation_requests
                
                # Additional context
                is_mobile_app,  # is_mobile_app
                page_load_time,  # page_load_time_ms
                is_conversion_event and converted,  # conversion_flag
                revenue_impact  # revenue_impact
            )
            
            # Set previous URL for next iteration
            previous_url = event_info['url']
$$;

CREATE OR REPLACE TABLE hospitality_events AS
SELECT e.*
FROM TABLE(GENERATOR(ROWCOUNT => $JOURNEY_COUNT)) g
CROSS JOIN TABLE(generate_hotel_journey()) e;

GRANT SELECT ON ALL TABLES IN SCHEMA SEQUENT_DB.HOSPITALITY TO ROLE SEQUENT_ROLE;

-- ===========================================================================
-- SECTION 9: ATTRIBUTION STORED PROCEDURES
-- ===========================================================================

USE SCHEMA SEQUENT_DB.ANALYTICS;

CREATE OR REPLACE PROCEDURE MARKOV_ATTRIBUTION_SP(
    paths_table STRING,
    path_column STRING,
    frequency_column STRING
)
RETURNS TABLE(channel STRING, attribution_pct FLOAT, removal_effect FLOAT, conversions FLOAT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'numpy', 'pandas')
HANDLER = 'run_markov_attribution'
AS
$$
import numpy as np
import pandas as pd
import re

def run_markov_attribution(session, paths_table, path_column, frequency_column):
    query = f"SELECT {path_column}, {frequency_column} FROM {paths_table}"
    df = session.sql(query).to_pandas()
    df.columns = ['path', 'frequency']
    regex = re.compile('[^a-zA-Z0-9>_ -]')
    df['path'] = df['path'].apply(lambda x: regex.sub('', str(x)))
    all_paths = []
    all_frequencies = []
    all_touchpoints = set()
    for _, row in df.iterrows():
        path_list = [tp.strip() for tp in row['path'].split(' > ')]
        freq = int(row['frequency'])
        all_paths.append(path_list)
        all_frequencies.append(freq)
        all_touchpoints.update(path_list)
    channels = [tp for tp in all_touchpoints if tp not in ['start', 'conv', 'null']]
    if not channels:
        from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
        schema = StructType([StructField("CHANNEL", StringType()), StructField("ATTRIBUTION_PCT", FloatType()), StructField("REMOVAL_EFFECT", FloatType()), StructField("CONVERSIONS", FloatType())])
        empty_result = pd.DataFrame([{'CHANNEL': 'No channels', 'ATTRIBUTION_PCT': 0.0, 'REMOVAL_EFFECT': 0.0, 'CONVERSIONS': 0.0}])
        return session.create_dataframe(empty_result, schema=schema)
    def build_transition_matrix(paths, frequencies, exclude_channel=None):
        unique_touch_list = set()
        for path in paths:
            unique_touch_list.update(path)
        filtered_paths = []
        for path in paths:
            if exclude_channel:
                filtered_path = [tp for tp in path if tp != exclude_channel]
                filtered_paths.append(filtered_path)
            else:
                filtered_paths.append(path)
        if exclude_channel and exclude_channel in unique_touch_list:
            unique_touch_list.remove(exclude_channel)
        transitionStates = {}
        for x in unique_touch_list:
            for y in unique_touch_list:
                transitionStates[x + ">" + y] = 0
        for possible_state in unique_touch_list:
            if possible_state != "null" and possible_state != "conv":
                for i, user_path in enumerate(filtered_paths):
                    freq = frequencies[i]
                    if possible_state in user_path:
                        indices = [j for j, s in enumerate(user_path) if possible_state == s]
                        for col in indices:
                            if col + 1 < len(user_path):
                                transitionStates[user_path[col] + ">" + user_path[col + 1]] += freq
        actual_paths = []
        for state in unique_touch_list:
            if state != "null" and state != "conv":
                counter = 0
                state_transitions = {k: v for k, v in transitionStates.items() if k.startswith(state + '>')}
                counter = sum(state_transitions.values())
                if counter > 0:
                    for trans, count in state_transitions.items():
                        if count > 0:
                            state_prob = float(count) / float(counter)
                            actual_paths.append({trans: state_prob})
        transState = []
        transMatrix = []
        for item in actual_paths:
            for key in item:
                transState.append(key)
                transMatrix.append(item[key])
        if not transState:
            return None, None, None
        tmatrix_df = pd.DataFrame({'paths': transState, 'prob': transMatrix})
        tmatrix_split = tmatrix_df['paths'].str.split('>', expand=True)
        tmatrix_df['channel0'] = tmatrix_split[0]
        tmatrix_df['channel1'] = tmatrix_split[1]
        test_df = pd.DataFrame(0.0, index=list(unique_touch_list), columns=list(unique_touch_list))
        for _, v in tmatrix_df.iterrows():
            x = v['channel0']
            y = v['channel1']
            val = v['prob']
            test_df.loc[x, y] = val
        test_df.loc['conv', 'conv'] = 1.0
        test_df.loc['null', 'null'] = 1.0
        return test_df, unique_touch_list, None
    def calculate_conversion_rate(test_df):
        R = test_df[['null', 'conv']]
        R = R.drop(['null', 'conv'], axis=0)
        Q = test_df.drop(['null', 'conv'], axis=1)
        Q = Q.drop(['null', 'conv'], axis=0)
        t = len(Q.columns)
        if t == 0:
            return 0.0
        try:
            N = np.linalg.inv(np.identity(t) - np.asarray(Q))
            M = np.dot(N, np.asarray(R))
            base_cvr = pd.DataFrame(M, index=R.index)[[1]].loc['start'].values[0]
            return base_cvr
        except:
            return 0.0
    def calculate_removals(df, base_cvr):
        removal_effect_list = dict()
        channels_to_remove = [col for col in df.columns if col not in ['conv', 'null', 'start']]
        for channel in channels_to_remove:
            removal_df = df.drop(channel, axis=1)
            removal_df = removal_df.drop(channel, axis=0)
            for col in removal_df.columns:
                if col not in ['null', 'conv']:
                    one = float(1)
                    row_sum = np.sum(list(removal_df.loc[col]))
                    null_percent = one - row_sum
                    if null_percent != 0:
                        removal_df.loc[col, 'null'] = null_percent
            removal_df.loc['null', 'null'] = 1.0
            R = removal_df[['null', 'conv']]
            R = R.drop(['null', 'conv'], axis=0)
            Q = removal_df.drop(['null', 'conv'], axis=1)
            Q = Q.drop(['null', 'conv'], axis=0)
            t = len(Q.columns)
            try:
                N = np.linalg.inv(np.identity(t) - np.asarray(Q))
                M = np.dot(N, np.asarray(R))
                removal_cvr = pd.DataFrame(M, index=R.index)[[1]].loc['start'].values[0]
                removal_effect = 1 - removal_cvr / base_cvr
                removal_effect_list[channel] = removal_effect
            except:
                removal_effect_list[channel] = 0.0
        return removal_effect_list
    test_df, unique_touch_list, _ = build_transition_matrix(all_paths, all_frequencies)
    if test_df is None:
        from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
        schema = StructType([StructField("CHANNEL", StringType()), StructField("ATTRIBUTION_PCT", FloatType()), StructField("REMOVAL_EFFECT", FloatType()), StructField("CONVERSIONS", FloatType())])
        equal_share = 100.0 / len(channels)
        equal_result = pd.DataFrame([{'CHANNEL': str(ch), 'ATTRIBUTION_PCT': float(equal_share), 'REMOVAL_EFFECT': 0.0, 'CONVERSIONS': 0.0} for ch in channels])
        return session.create_dataframe(equal_result, schema=schema)
    base_cvr = calculate_conversion_rate(test_df)
    if base_cvr == 0:
        from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
        schema = StructType([StructField("CHANNEL", StringType()), StructField("ATTRIBUTION_PCT", FloatType()), StructField("REMOVAL_EFFECT", FloatType()), StructField("CONVERSIONS", FloatType())])
        equal_share = 100.0 / len(channels)
        equal_result = pd.DataFrame([{'CHANNEL': str(ch), 'ATTRIBUTION_PCT': float(equal_share), 'REMOVAL_EFFECT': 0.0, 'CONVERSIONS': 0.0} for ch in channels])
        return session.create_dataframe(equal_result, schema=schema)
    removal_effects = calculate_removals(test_df, base_cvr)
    denominator = np.sum(list(removal_effects.values()))
    total_conversions = sum(all_frequencies)
    if denominator > 0:
        attribution_pcts = {ch: (removal_effects[ch] / denominator) * 100 for ch in channels}
        conversions = {ch: (removal_effects[ch] / denominator) * total_conversions for ch in channels}
    else:
        equal_share = 100.0 / len(channels)
        attribution_pcts = {ch: equal_share for ch in channels}
        conversions = {ch: (equal_share / 100) * total_conversions for ch in channels}
    result = pd.DataFrame([{'CHANNEL': str(ch), 'ATTRIBUTION_PCT': float(attribution_pcts[ch]), 'REMOVAL_EFFECT': float(removal_effects.get(ch, 0)), 'CONVERSIONS': float(conversions[ch])} for ch in channels])
    result_sorted = result.sort_values('ATTRIBUTION_PCT', ascending=False)
    from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
    schema = StructType([StructField("CHANNEL", StringType()), StructField("ATTRIBUTION_PCT", FloatType()), StructField("REMOVAL_EFFECT", FloatType()), StructField("CONVERSIONS", FloatType())])
    return session.create_dataframe(result_sorted, schema=schema)
$$;

CREATE OR REPLACE PROCEDURE SHAPLEY_ATTRIBUTION_SP(
    paths_table STRING,
    path_column STRING,
    frequency_column STRING,
    n_samples INTEGER
)
RETURNS TABLE(channel STRING, shapley_value FLOAT, attribution_pct FLOAT, conversions FLOAT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'numpy', 'pandas')
HANDLER = 'run_shapley_attribution'
AS
$$
import numpy as np
import pandas as pd

def run_shapley_attribution(session, paths_table, path_column, frequency_column, n_samples):
    query = f"SELECT {path_column}, {frequency_column} FROM {paths_table}"
    df = session.sql(query).to_pandas()
    df.columns = ['path', 'frequency']
    all_touchpoints = set()
    path_data = []
    for _, row in df.iterrows():
        touchpoints = [tp.strip() for tp in str(row['path']).split(',')]
        freq = int(row['frequency'])
        path_data.append((touchpoints, freq))
        all_touchpoints.update(touchpoints)
    channels = sorted(list(all_touchpoints))
    if not channels:
        from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
        schema = StructType([StructField("CHANNEL", StringType()), StructField("SHAPLEY_VALUE", FloatType()), StructField("ATTRIBUTION_PCT", FloatType()), StructField("CONVERSIONS", FloatType())])
        empty_result = pd.DataFrame([{'CHANNEL': 'No channels', 'SHAPLEY_VALUE': 0.0, 'ATTRIBUTION_PCT': 0.0, 'CONVERSIONS': 0.0}])
        return session.create_dataframe(empty_result, schema=schema)
    def coalition_value(coalition, path_data):
        if not coalition:
            return 0.0
        coalition_set = set(coalition)
        matched_conversions = 0
        total_freq = 0
        for touchpoints, freq in path_data:
            if any(tp in coalition_set for tp in touchpoints):
                matched_conversions += freq
            total_freq += freq
        return matched_conversions / total_freq if total_freq > 0 else 0.0
    shapley_values = {ch: 0.0 for ch in channels}
    for _ in range(n_samples):
        permutation = np.random.permutation(channels)
        for i, channel in enumerate(permutation):
            coalition_without = set(permutation[:i])
            coalition_with = coalition_without | {channel}
            value_without = coalition_value(coalition_without, path_data)
            value_with = coalition_value(coalition_with, path_data)
            marginal = value_with - value_without
            shapley_values[channel] += marginal
    for ch in shapley_values:
        shapley_values[ch] /= n_samples
    abs_values = {ch: abs(val) for ch, val in shapley_values.items()}
    total_abs = sum(abs_values.values())
    if total_abs > 0:
        attribution_pcts = {ch: (abs_values[ch] / total_abs) * 100 for ch in channels}
    else:
        equal_share = 100.0 / len(channels)
        attribution_pcts = {ch: equal_share for ch in channels}
    total_conversions = sum(freq for _, freq in path_data)
    conversions = {ch: (pct / 100) * total_conversions for ch, pct in attribution_pcts.items()}
    result = pd.DataFrame([{'CHANNEL': str(ch), 'SHAPLEY_VALUE': float(shapley_values[ch]), 'ATTRIBUTION_PCT': float(attribution_pcts[ch]), 'CONVERSIONS': float(conversions[ch])} for ch in channels])
    result_sorted = result.sort_values('ATTRIBUTION_PCT', ascending=False)
    from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
    schema = StructType([StructField("CHANNEL", StringType()), StructField("SHAPLEY_VALUE", FloatType()), StructField("ATTRIBUTION_PCT", FloatType()), StructField("CONVERSIONS", FloatType())])
    return session.create_dataframe(result_sorted, schema=schema)
$$;

GRANT USAGE ON ALL PROCEDURES IN SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;

-- Additional grants for dynamic procedure creation (AttributionAnalysis fallback)
GRANT CREATE PROCEDURE ON SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;
GRANT ALL ON FUTURE PROCEDURES IN SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;

-- Model Registry permissions for PredictiveModeling
GRANT CREATE MODEL ON SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;
GRANT ALL ON FUTURE MODELS IN SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;

-- ===========================================================================
-- SECTION 10: ANALYTICS VIEWS
-- ===========================================================================

CREATE OR REPLACE VIEW all_events_unified AS
SELECT 'Retail' AS industry, customer_id AS user_id, event_type AS event_name, event_timestamp, customer_segment AS segment, conversion_flag, revenue_impact
FROM SEQUENT_DB.RETAIL.retail_events;

CREATE OR REPLACE VIEW journey_statistics AS
SELECT customer_id, COUNT(DISTINCT event_type) AS unique_events, COUNT(*) AS total_events, MIN(event_timestamp) AS journey_start, MAX(event_timestamp) AS journey_end,
TIMEDIFF(SECOND, MIN(event_timestamp), MAX(event_timestamp)) AS journey_duration_seconds, MAX(conversion_flag::INT) AS converted, MAX(revenue_impact) AS total_revenue
FROM SEQUENT_DB.RETAIL.retail_events GROUP BY customer_id;

CREATE OR REPLACE VIEW retail_conversion_funnel AS
SELECT customer_id, 
MAX(CASE WHEN event_type = 'homepage_visit' THEN 1 ELSE 0 END) AS reached_homepage,
MAX(CASE WHEN event_type = 'product_detail_view' THEN 1 ELSE 0 END) AS reached_product_view,
MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS reached_add_to_cart,
MAX(CASE WHEN event_type = 'checkout_initiation' THEN 1 ELSE 0 END) AS reached_checkout,
MAX(CASE WHEN event_type = 'purchase_completion' THEN 1 ELSE 0 END) AS completed_purchase
FROM SEQUENT_DB.RETAIL.retail_events GROUP BY customer_id;

GRANT SELECT ON ALL VIEWS IN SCHEMA SEQUENT_DB.ANALYTICS TO ROLE SEQUENT_ROLE;

-- ===========================================================================
-- SETUP COMPLETE
-- ===========================================================================
-- Database, schemas, sample data, stored procedures, and views are now ready.
-- 
-- To deploy the Streamlit app, run: scripts/deploy_streamlit.sql
-- ===========================================================================
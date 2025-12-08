# Customer Journey Analytics with Sequent™

## Overview

Sequent™ native application allows users to easily and visually perform and deep dive into Path Analysis, Attribution Analysis, Association Analysis, Pattern Mining, Behavioral Segmentation and Predictive Modeling by simply specifying a few parameters in drop-down menus. Leveraging advanced techniques, Sequent™ intuitively and visually helps identify touchpoints influencing customer (or machine) behaviours, targets them to create segments, performs cross-population behavioural comparisons, computes rule-based and ML-driven attribution models to understand the contribution of each event preceding a specific outcome, conducts association analysis to uncover hidden patterns and relationships between events, discovers frequent sequential patterns and behavioral signatures through advanced pattern mining, and enables sophisticated behavioral segmentation to group customers based on their journey patterns and characteristics. Sequent™ also harnesses the interpretive and generative power of LLMs thanks to Snowflake AISQL to explain journeys, attribution models, association rules, pattern insights and derive insights (summarize and analyze results, describe behaviors and even suggest actions!)

Visualizing and identifying paths can itself be actionable and often uncovers an area of interest for additional analysis. First, the picture revealed by path analysis can be further enriched with attribution analysis, association analysis, pattern mining, and behavioral segmentation. Attribution helps quantify the contribution of individual touchpoints to a defined outcome, association analysis uncovers relationships between events that frequently occur together, pattern mining discovers frequent sequential behaviors and hidden temporal dependencies, and behavioral segmentation groups customers into meaningful clusters based on their journey characteristics and patterns. Together, these techniques provide a comprehensive understanding of event sequences, enabling data-driven decision-making and uncovering new opportunities for optimization. Second, path insights can be used directly to predict outcomes (Predictive Modeling) or to derive behavioral features (such as the frequency of specific patterns and sequence signatures). These features can then be integrated into existing predictive models, enhancing their accuracy and enabling deeper customer understanding through advanced segmentation strategies.

## Repository Structure

```
├── README.md                              # This file
├── LEGAL.md                               # Legal information
├── LICENSE                                # License file
├── scripts/
│   ├── setup.sql                          # Database, data, and stored procedures setup
│   ├── deploy_streamlit.sql               # Sequent Streamlit app deployment via Git integration
│   └── teardown.sql                       # Cleanup script to remove all resources
├── streamlit/
│   ├── sequent.py                         # Sequent main application (landing page)
│   ├── environment.yml                    # Python dependencies
│   ├── assets/
│   │   └── Sequent.png
│   └── pages/                             # 6 analytics page modules
│       ├── AssociationAnalysis.py
│       ├── AttributionAnalysis.py
│       ├── BehavioralSegmentation.py
│       ├── PathAnalysis.py
│       ├── PatternMining.py
│       └── PredictiveModeling.py
```

## Getting Started

### Prerequisites
- Snowflake account (trial accounts supported)
- ACCOUNTADMIN role access

### Simple Setup

**Step 1: Run the Setup Script**
1. Open [`setup.sql`](scripts/setup.sql) and copy the entire contents
2. Open **Snowflake Snowsight** (https://app.snowflake.com)
3. Create a **new SQL Worksheet**
4. **Paste the contents** and click **Run All** 

The setup script will automatically:
- ✅ Create a Snowpark-optimized warehouse (MEDIUM size)
- ✅ Generate 5 industry datasets (~100K customer journeys per industry)
  - Retail, Financial Services, Hospitality, Gaming, Food Delivery
- ✅ Create stored procedures for Markov Chain and Shapley Value attribution
- ✅ Configure all necessary permissions and grants

**Step 2: Deploy the Streamlit App**
1. Open [`deploy_streamlit.sql`](scripts/deploy_streamlit.sql) and copy the entire contents
2. Create a **new SQL Worksheet**
3. **Paste the contents** and click **Run All**

This deploys the Streamlit app via Git integration from this repository.

**Step 3: Access the Applications**

Once deployment completes:
1. In Snowsight, **switch to `SEQUENT_ROLE`** using the role selector (top-left)
2. Navigate to **Projects** → **Streamlit** (left sidebar)
3. You'll see **"Sequent"**
4. Click to open and start exploring!

> **Note:** The setup script automatically grants `SEQUENT_ROLE` to the user who runs it. This role has all necessary permissions for the analytics applications.

**All 6 analytics applications are now available:**
- Path Analysis
- Attribution Modeling
- Behavioral Segmentation
- Association Analysis
- Pattern Mining
- Predictive Modeling

## Cleanup

To remove all resources created by this quickstart, run [`teardown.sql`](scripts/teardown.sql) in a SQL Worksheet. This will drop the database, warehouse, role, and Streamlit app.
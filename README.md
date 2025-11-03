COINSIFTER/
├── 📁 analysis/
│   ├── 🐍 constants.py
│   ├── 🐍 helpers.py
│   ├── 🐍 logic.py
│   ├── 🐍 stage_0_prereqs.py
│   ├── 🐍 stage_2_maturity.py
│   └── 🐍 stage_3_analysis_workers.py
├── 📁 api/
│   ├── 📁 endpoints/
│   │   ├── 🐍 blacklist.py
│   │   ├── 🐍 coins.py
│   │   ├── 🐍 health.py
│   │   ├── 🐍 logs.py
│   │   └── 🐍 trigger.py
│   ├── 🐍 router.py
│   └── 🐍 security.py
├── 📁 database/
│   ├── 🐍 __init__.py
│   ├── 🐍 coins.py
│   ├── 🐍 connection.py
│   ├── 🐍 logs.py
│   ├── 🐍 schema.py
│   └── 🐍 utils.py
├── 📁 metrics/
│   ├── 🐍 calculator.py
│   ├── 🐍 harmony.py
│   ├── 🐍 market.py
│   ├── 🐍 ranking.py
│   ├── 🐍 structure.py
│   ├── 🐍 technical.py
│   └── 🐍 utils.py
├── 📁 services/
│   ├── 🐍 __init__.py
│   ├── 🐍 data_fetcher.py
│   ├── 🐍 exchange_api.py
│   ├── ⚫ exchange_utils.py
│   └── 🐍 mongo_service.py
├── 🐍 config.py
├── 🐍 main.py
├── 🐍 router.py  (Файл `router.py` в КОРНЕ, который вы мне давали)
├── 📄 structure.md
├── 🐍 test_api.py
├── 🐍 test_analysis.py
└── 🐍 test_db.py
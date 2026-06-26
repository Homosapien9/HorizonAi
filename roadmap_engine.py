"""
Horizon v2 - Roadmap Engine (Upgraded)
150+ skill graph, dynamic track generation, live-data-driven insights, gap analysis, salary bands, DAG export.
"""
from __future__ import annotations
import asyncio
import logging
import re
import functools
import json
import time
import uuid
from pathlib import Path

import models
from collections import defaultdict, deque
from datetime import UTC, datetime
from typing import Optional

from models import (
    DAGEdge,
    Internship,
    ComparisonResult,
    RoadmapRequest,
    MarketDemand,
    Phase,
    RoadmapResult,
    SalaryBand,
    Scholarship,
    SkillLevel,
    SkillNode,
    TopCompany,
    TrendAnalysis,
    University,
)

from career_intelligence_pipeline import (
    Analyzer,
    DynamicTrackGenerator,
    _get_embed_model,
    _get_kw_model,
    extract_skills_regex,
    fetch_all_market_data,
)

logger = logging.getLogger(__name__)

# ── 150+ Skill Graph ──────────────────────────────────────────────────────────

SKILL_GRAPH: dict[str, dict] = {

    # ═══════════════════════════════════════════════════════════════════════════
    # FOUNDATIONS (shared across all tracks)
    # ═══════════════════════════════════════════════════════════════════════════
    "git": {
        "hours": 20, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Pro Git (free book)", "url": "https://git-scm.com/book/en/v2"},
            {"title": "GitHub Skills", "url": "https://skills.github.com/"},
            {"title": "Oh My Git! (interactive)", "url": "https://ohmygit.org/"},
        ],
        "why": "Version control is the non-negotiable baseline for every software career.",
        "tracks": ["ml","ai_research","data_science","backend","frontend","cybersecurity","devops","bioinformatics"],
        "salary_impact": "+$3k",
    },
    "linux": {
        "hours": 30, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Linux Journey", "url": "https://linuxjourney.com/"},
            {"title": "The Linux Command Line (free)", "url": "https://linuxcommand.org/tlcl.php"},
            {"title": "OverTheWire: Bandit", "url": "https://overthewire.org/wargames/bandit/"},
        ],
        "why": "Linux underpins servers, ML clusters, containers, and all DevOps tooling.",
        "tracks": ["ml","ai_research","backend","cybersecurity","devops","bioinformatics"],
        "salary_impact": "+$5k",
    },
    "python": {
        "hours": 60, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Python Official Tutorial", "url": "https://docs.python.org/3/tutorial/"},
            {"title": "Automate the Boring Stuff (free)", "url": "https://automatetheboringstuff.com/"},
            {"title": "MIT 6.0001 OCW", "url": "https://ocw.mit.edu/courses/6-0001-introduction-to-computer-science-and-programming-in-python-fall-2016/"},
            {"title": "Python for Everybody", "url": "https://www.py4e.com/"},
        ],
        "why": "Python dominates ML, data science, automation, and backend development.",
        "tracks": ["ml","ai_research","data_science","backend","cybersecurity","devops","bioinformatics"],
        "salary_impact": "+$18k",
    },
    "javascript": {
        "hours": 60, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "javascript.info", "url": "https://javascript.info/"},
            {"title": "MDN Web Docs", "url": "https://developer.mozilla.org/en-US/docs/Web/JavaScript"},
            {"title": "freeCodeCamp JS", "url": "https://www.freecodecamp.org/learn/javascript-algorithms-and-data-structures/"},
        ],
        "why": "JavaScript is the language of the web - essential for all frontend work.",
        "tracks": ["frontend"],
        "salary_impact": "+$14k",
    },
    "typescript": {
        "hours": 30, "phase": "core", "prerequisites": ["javascript"],
        "resources": [
            {"title": "TypeScript Handbook", "url": "https://www.typescriptlang.org/docs/handbook/"},
            {"title": "Total TypeScript", "url": "https://www.totaltypescript.com/"},
            {"title": "Execute Program TypeScript", "url": "https://www.executeprogram.com/courses/typescript"},
        ],
        "why": "TypeScript is standard in production frontend codebases - required by most employers.",
        "tracks": ["frontend"],
        "salary_impact": "+$10k",
    },
    "html_css": {
        "hours": 40, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "MDN HTML", "url": "https://developer.mozilla.org/en-US/docs/Learn/HTML"},
            {"title": "CSS-Tricks", "url": "https://css-tricks.com/"},
            {"title": "The Odin Project (free)", "url": "https://www.theodinproject.com/paths/foundations"},
        ],
        "why": "HTML and CSS are the foundation of every web interface.",
        "tracks": ["frontend"],
        "salary_impact": "+$4k",
    },
    "statistics": {
        "hours": 50, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Khan Academy Statistics", "url": "https://www.khanacademy.org/math/statistics-probability"},
            {"title": "StatQuest (YouTube)", "url": "https://www.youtube.com/c/joshstarmer"},
            {"title": "Think Stats 2e (free)", "url": "https://greenteapress.com/wp/think-stats-2e/"},
            {"title": "Introduction to Statistical Learning (free)", "url": "https://www.statlearning.com/"},
        ],
        "why": "Statistical reasoning is the foundation of all data-driven work.",
        "tracks": ["ml","ai_research","data_science","bioinformatics"],
        "salary_impact": "+$12k",
    },
    "linear_algebra": {
        "hours": 40, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "3Blue1Brown Essence of Linear Algebra", "url": "https://www.youtube.com/playlist?list=PLZHQObOWTQDPD3MizzM2xVFitgF8hE_ab"},
            {"title": "MIT 18.06 OCW", "url": "https://ocw.mit.edu/courses/18-06-linear-algebra-spring-2010/"},
            {"title": "Immersive Linear Algebra (free)", "url": "http://immersivemath.com/ila/index.html"},
        ],
        "why": "Linear algebra is the mathematical engine behind every ML algorithm.",
        "tracks": ["ml","ai_research","data_science"],
        "salary_impact": "+$8k",
    },
    "calculus": {
        "hours": 35, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Khan Academy Calculus", "url": "https://www.khanacademy.org/math/calculus-1"},
            {"title": "3Blue1Brown Essence of Calculus", "url": "https://www.youtube.com/playlist?list=PLZHQObOWTQDMsr9K-rj53DwVRMYO3t5Yr"},
            {"title": "MIT 18.01 OCW", "url": "https://ocw.mit.edu/courses/18-01sc-single-variable-calculus-fall-2010/"},
        ],
        "why": "Calculus underpins gradient descent - the core of neural network training.",
        "tracks": ["ml","ai_research"],
        "salary_impact": "+$6k",
    },
    "probability": {
        "hours": 30, "phase": "foundation", "prerequisites": ["statistics"],
        "resources": [
            {"title": "MIT 6.041 Probabilistic Systems", "url": "https://ocw.mit.edu/courses/6-041-probabilistic-systems-analysis-and-applied-probability-fall-2010/"},
            {"title": "Seeing Theory (visual)", "url": "https://seeing-theory.brown.edu/"},
        ],
        "why": "Probability theory underlies Bayesian ML, reinforcement learning, and model evaluation.",
        "tracks": ["ml","ai_research","data_science"],
        "salary_impact": "+$7k",
    },
    "sql": {
        "hours": 40, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "SQLZoo", "url": "https://sqlzoo.net/"},
            {"title": "Mode SQL Tutorial", "url": "https://mode.com/sql-tutorial/"},
            {"title": "pgexercises", "url": "https://pgexercises.com/"},
            {"title": "Select Star SQL (free)", "url": "https://selectstarsql.com/"},
        ],
        "why": "SQL is the universal language of data - required in virtually every data role.",
        "tracks": ["ml","data_science","backend","bioinformatics"],
        "salary_impact": "+$9k",
    },
    "data_structures_algorithms": {
        "hours": 80, "phase": "foundation", "prerequisites": ["python"],
        "resources": [
            {"title": "CS50 Harvard (free)", "url": "https://cs50.harvard.edu/x/"},
            {"title": "NeetCode", "url": "https://neetcode.io/"},
            {"title": "MIT 6.006 OCW", "url": "https://ocw.mit.edu/courses/6-006-introduction-to-algorithms-fall-2011/"},
            {"title": "Visualgo (interactive)", "url": "https://visualgo.net/"},
        ],
        "why": "DSA is tested in every technical interview and underpins efficient software.",
        "tracks": ["ml","ai_research","backend","frontend","cybersecurity"],
        "salary_impact": "+$15k",
    },
    "communication_skills": {
        "hours": 20, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Google Technical Writing Course (free)", "url": "https://developers.google.com/tech-writing"},
            {"title": "Google Technical Writing One", "url": "https://developers.google.com/tech-writing/one"},
        ],
        "why": "Clear technical communication is as valuable as code in every STEM career.",
        "tracks": ["ml","ai_research","data_science","backend","frontend","cybersecurity","devops","bioinformatics"],
        "salary_impact": "+$5k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # MACHINE LEARNING TRACK
    # ═══════════════════════════════════════════════════════════════════════════
    "numpy": {
        "hours": 20, "phase": "core", "prerequisites": ["python"],
        "resources": [
            {"title": "NumPy Official Tutorial", "url": "https://numpy.org/doc/stable/user/quickstart.html"},
            {"title": "NumPy for Absolute Beginners", "url": "https://numpy.org/doc/stable/user/absolute_beginners.html"},
        ],
        "why": "NumPy is the numerical computing bedrock of the entire Python ML ecosystem.",
        "tracks": ["ml","data_science","bioinformatics"],
        "salary_impact": "+$4k",
    },
    "pandas": {
        "hours": 25, "phase": "core", "prerequisites": ["numpy"],
        "resources": [
            {"title": "Pandas Official Docs", "url": "https://pandas.pydata.org/docs/getting_started/index.html"},
            {"title": "Kaggle Pandas Course", "url": "https://www.kaggle.com/learn/pandas"},
            {"title": "Pandas Exercises (GitHub)", "url": "https://github.com/guipsamora/pandas_exercises"},
        ],
        "why": "Pandas is the standard for data wrangling and exploratory analysis.",
        "tracks": ["ml","data_science","bioinformatics"],
        "salary_impact": "+$5k",
    },
    "matplotlib_seaborn": {
        "hours": 15, "phase": "core", "prerequisites": ["pandas"],
        "resources": [
            {"title": "Matplotlib Tutorials", "url": "https://matplotlib.org/stable/tutorials/index.html"},
            {"title": "Seaborn Tutorial", "url": "https://seaborn.pydata.org/tutorial.html"},
            {"title": "Python Graph Gallery", "url": "https://python-graph-gallery.com/"},
        ],
        "why": "Data visualization is critical for communicating findings and debugging models.",
        "tracks": ["ml","data_science"],
        "salary_impact": "+$3k",
    },
    "scikit_learn": {
        "hours": 40, "phase": "core", "prerequisites": ["numpy","pandas","statistics"],
        "resources": [
            {"title": "scikit-learn User Guide", "url": "https://scikit-learn.org/stable/user_guide.html"},
            {"title": "Kaggle ML Course", "url": "https://www.kaggle.com/learn/intro-to-machine-learning"},
            {"title": "ML Course.ai", "url": "https://mlcourse.ai/book/index.html"},
        ],
        "why": "scikit-learn is the go-to library for classical ML - every practitioner uses it.",
        "tracks": ["ml","data_science"],
        "salary_impact": "+$8k",
    },
    "feature_engineering": {
        "hours": 30, "phase": "core", "prerequisites": ["scikit_learn"],
        "resources": [
            {"title": "Feature Engineering for ML (Kaggle)", "url": "https://www.kaggle.com/learn/feature-engineering"},
            {"title": "Featuretools Docs", "url": "https://featuretools.alteryx.com/en/stable/"},
        ],
        "why": "Feature quality often matters more than model choice - this separates practitioners from competition winners.",
        "tracks": ["ml","data_science"],
        "salary_impact": "+$6k",
    },
    "deep_learning": {
        "hours": 80, "phase": "specialization", "prerequisites": ["scikit_learn","linear_algebra","calculus"],
        "resources": [
            {"title": "fast.ai Practical Deep Learning", "url": "https://course.fast.ai/"},
            {"title": "Dive into Deep Learning", "url": "https://d2l.ai/"},
            {"title": "MIT 6.S191 Deep Learning", "url": "http://introtodeeplearning.mit.edu/"},
        ],
        "why": "Deep learning is the engine of modern AI across vision, NLP, speech, and more.",
        "tracks": ["ml","ai_research"],
        "salary_impact": "+$22k",
    },
    "pytorch": {
        "hours": 50, "phase": "specialization", "prerequisites": ["deep_learning"],
        "resources": [
            {"title": "PyTorch Official Tutorials", "url": "https://pytorch.org/tutorials/"},
            {"title": "Zero to Mastery PyTorch (free)", "url": "https://www.learnpytorch.io/"},
            {"title": "UvA Deep Learning (free)", "url": "https://uvadlc-notebooks.readthedocs.io/"},
        ],
        "why": "PyTorch dominates research and is increasingly standard in production ML.",
        "tracks": ["ml","ai_research"],
        "salary_impact": "+$18k",
    },
    "tensorflow": {
        "hours": 40, "phase": "specialization", "prerequisites": ["deep_learning"],
        "resources": [
            {"title": "TensorFlow Official Tutorials", "url": "https://www.tensorflow.org/tutorials"},
            {"title": "Google ML Crash Course (free)", "url": "https://developers.google.com/machine-learning/crash-course"},
        ],
        "why": "TensorFlow / Keras is widely deployed in production at Google-scale systems.",
        "tracks": ["ml"],
        "salary_impact": "+$15k",
    },
    "natural_language_processing": {
        "hours": 60, "phase": "specialization", "prerequisites": ["deep_learning"],
        "resources": [
            {"title": "Stanford CS224N NLP (free)", "url": "https://web.stanford.edu/class/cs224n/"},
            {"title": "HuggingFace NLP Course (free)", "url": "https://huggingface.co/learn/nlp-course/chapter1/1"},
            {"title": "Lena Voita NLP Course (free)", "url": "https://lena-voita.github.io/nlp_course.html"},
        ],
        "why": "NLP powers LLMs, search, summarization, and every text-based AI application.",
        "tracks": ["ml","ai_research"],
        "salary_impact": "+$20k",
    },
    "computer_vision": {
        "hours": 60, "phase": "specialization", "prerequisites": ["deep_learning"],
        "resources": [
            {"title": "Stanford CS231n (free)", "url": "http://cs231n.stanford.edu/"},
            {"title": "OpenCV Python Tutorial", "url": "https://docs.opencv.org/4.x/d6/d00/tutorial_py_root.html"},
            {"title": "Roboflow Notebooks", "url": "https://github.com/roboflow/notebooks"},
        ],
        "why": "Computer vision enables image recognition, medical imaging AI, and autonomous systems.",
        "tracks": ["ml","ai_research"],
        "salary_impact": "+$20k",
    },
    "mlops": {
        "hours": 50, "phase": "advanced", "prerequisites": ["pytorch","docker"],
        "resources": [
            {"title": "Made With ML", "url": "https://madewithml.com/"},
            {"title": "MLflow Documentation", "url": "https://mlflow.org/docs/latest/index.html"},
            {"title": "Weights & Biases Courses (free)", "url": "https://www.wandb.courses/"},
            {"title": "Full Stack Deep Learning", "url": "https://fullstackdeeplearning.com/"},
        ],
        "why": "MLOps bridges prototype and production - the most in-demand ML skill gap.",
        "tracks": ["ml"],
        "salary_impact": "+$25k",
    },
    "model_deployment": {
        "hours": 35, "phase": "advanced", "prerequisites": ["mlops","fastapi_fw"],
        "resources": [
            {"title": "BentoML Docs", "url": "https://docs.bentoml.com/en/latest/"},
            {"title": "ONNX Tutorial", "url": "https://onnxruntime.ai/docs/get-started/with-python.html"},
            {"title": "Triton Inference Server", "url": "https://developer.nvidia.com/triton-inference-server"},
        ],
        "why": "Getting models into production is the hardest part - and what companies actually pay for.",
        "tracks": ["ml"],
        "salary_impact": "+$18k",
    },
    "experiment_tracking": {
        "hours": 20, "phase": "specialization", "prerequisites": ["scikit_learn"],
        "resources": [
            {"title": "MLflow Quickstart", "url": "https://mlflow.org/docs/latest/quickstart.html"},
            {"title": "DVC Tutorial", "url": "https://dvc.org/doc/start"},
        ],
        "why": "Reproducible experiments are a professional standard in ML teams.",
        "tracks": ["ml","ai_research"],
        "salary_impact": "+$6k",
    },
    "time_series": {
        "hours": 30, "phase": "specialization", "prerequisites": ["pandas","statistics"],
        "resources": [
            {"title": "Forecasting: Principles and Practice (free)", "url": "https://otexts.com/fpp3/"},
            {"title": "Darts Documentation", "url": "https://unit8co.github.io/darts/"},
        ],
        "why": "Time-series forecasting is core to finance, operations, and IoT ML.",
        "tracks": ["ml","data_science"],
        "salary_impact": "+$10k",
    },
    "recommendation_systems": {
        "hours": 35, "phase": "advanced", "prerequisites": ["deep_learning","feature_engineering"],
        "resources": [
            {"title": "Google Recsys Course (free)", "url": "https://developers.google.com/machine-learning/recommendation"},
            {"title": "Surprise Library", "url": "https://surpriselib.com/"},
        ],
        "why": "Recommendation systems power every major consumer tech product.",
        "tracks": ["ml"],
        "salary_impact": "+$15k",
    },
    "xgboost_lightgbm": {
        "hours": 20, "phase": "core", "prerequisites": ["scikit_learn"],
        "resources": [
            {"title": "XGBoost Docs", "url": "https://xgboost.readthedocs.io/en/stable/"},
            {"title": "LightGBM Docs", "url": "https://lightgbm.readthedocs.io/"},
        ],
        "why": "Gradient boosting wins tabular-data Kaggle competitions and is widely used in industry.",
        "tracks": ["ml","data_science"],
        "salary_impact": "+$8k",
    },
    "model_interpretability": {
        "hours": 20, "phase": "advanced", "prerequisites": ["scikit_learn"],
        "resources": [
            {"title": "SHAP Documentation", "url": "https://shap.readthedocs.io/"},
            {"title": "Interpretable ML Book (free)", "url": "https://christophm.github.io/interpretable-ml-book/"},
        ],
        "why": "Explainable AI is increasingly required in regulated industries.",
        "tracks": ["ml","data_science"],
        "salary_impact": "+$8k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # AI RESEARCH TRACK
    # ═══════════════════════════════════════════════════════════════════════════
    "transformers_arch": {
        "hours": 60, "phase": "specialization", "prerequisites": ["natural_language_processing"],
        "resources": [
            {"title": "HuggingFace Transformers Docs", "url": "https://huggingface.co/docs/transformers/index"},
            {"title": "The Annotated Transformer", "url": "https://nlp.seas.harvard.edu/annotated-transformer/"},
            {"title": "Andrej Karpathy's nanoGPT", "url": "https://github.com/karpathy/nanoGPT"},
        ],
        "why": "Transformers are the architecture behind every major AI breakthrough since 2017.",
        "tracks": ["ai_research"],
        "salary_impact": "+$28k",
    },
    "reinforcement_learning": {
        "hours": 70, "phase": "specialization", "prerequisites": ["deep_learning","statistics","probability"],
        "resources": [
            {"title": "Sutton & Barto RL Book (free)", "url": "http://incompleteideas.net/book/RLbook2020.pdf"},
            {"title": "Spinning Up in Deep RL (OpenAI)", "url": "https://spinningup.openai.com/en/latest/"},
            {"title": "David Silver RL Course (YouTube)", "url": "https://www.youtube.com/playlist?list=PLweqsIcZJac7PfiyYMvYiHfOFPg9Um82B"},
        ],
        "why": "RL is behind robotics, game AI, RLHF for LLM alignment, and autonomous systems.",
        "tracks": ["ai_research"],
        "salary_impact": "+$25k",
    },
    "research_methods": {
        "hours": 30, "phase": "core", "prerequisites": [],
        "resources": [
            {"title": "How to Read a Paper (Stanford)", "url": "https://web.stanford.edu/class/ee384m/Handouts/HowtoReadPaper.pdf"},
            {"title": "arXiv.org", "url": "https://arxiv.org/"},
            {"title": "Papers With Code", "url": "https://paperswithcode.com/"},
            {"title": "Distill.pub", "url": "https://distill.pub/"},
        ],
        "why": "Reading, reproducing, and extending research papers is the core skill of an AI researcher.",
        "tracks": ["ai_research"],
        "salary_impact": "+$5k",
    },
    "latex": {
        "hours": 15, "phase": "core", "prerequisites": [],
        "resources": [
            {"title": "Overleaf Learn LaTeX", "url": "https://www.overleaf.com/learn"},
            {"title": "LaTeX Tutorial", "url": "https://www.latex-tutorial.com/"},
        ],
        "why": "LaTeX is the standard for writing and publishing academic papers.",
        "tracks": ["ai_research","bioinformatics"],
        "salary_impact": "+$2k",
    },
    "graph_neural_networks": {
        "hours": 45, "phase": "advanced", "prerequisites": ["deep_learning","data_structures_algorithms"],
        "resources": [
            {"title": "Stanford CS224W GNN Course (free)", "url": "http://web.stanford.edu/class/cs224w/"},
            {"title": "PyG Documentation", "url": "https://pytorch-geometric.readthedocs.io/en/latest/"},
            {"title": "DGL Tutorial", "url": "https://docs.dgl.ai/tutorials/blitz/index.html"},
        ],
        "why": "GNNs are state-of-the-art for molecular, social network, and knowledge graph tasks.",
        "tracks": ["ai_research"],
        "salary_impact": "+$20k",
    },
    "diffusion_models": {
        "hours": 40, "phase": "advanced", "prerequisites": ["deep_learning","pytorch"],
        "resources": [
            {"title": "Lilian Weng Diffusion Blog", "url": "https://lilianweng.github.io/posts/2021-07-11-diffusion-models/"},
            {"title": "HuggingFace Diffusers Tutorial", "url": "https://huggingface.co/docs/diffusers/index"},
            {"title": "Annotated Diffusion Model", "url": "https://huggingface.co/blog/annotated-diffusion"},
        ],
        "why": "Diffusion models power state-of-the-art image, video, and audio generation.",
        "tracks": ["ai_research"],
        "salary_impact": "+$22k",
    },
    "distributed_training": {
        "hours": 35, "phase": "advanced", "prerequisites": ["pytorch"],
        "resources": [
            {"title": "PyTorch DDP Tutorial", "url": "https://pytorch.org/tutorials/intermediate/ddp_tutorial.html"},
            {"title": "DeepSpeed Documentation", "url": "https://www.deepspeed.ai/"},
            {"title": "Megatron-LM", "url": "https://github.com/NVIDIA/Megatron-LM"},
        ],
        "why": "Training large models requires mastery of distributed computation strategies.",
        "tracks": ["ai_research"],
        "salary_impact": "+$20k",
    },
    "fine_tuning_llms": {
        "hours": 40, "phase": "advanced", "prerequisites": ["transformers_arch"],
        "resources": [
            {"title": "HuggingFace PEFT Library", "url": "https://huggingface.co/docs/peft/index"},
            {"title": "LoRA Paper", "url": "https://arxiv.org/abs/2106.09685"},
            {"title": "Axolotl Fine-tuning Tool", "url": "https://github.com/OpenAccess-AI-Collective/axolotl"},
        ],
        "why": "Fine-tuning open-source LLMs on domain data is the dominant AI engineering task of this era.",
        "tracks": ["ai_research","ml"],
        "salary_impact": "+$30k",
    },
    "multimodal_ai": {
        "hours": 40, "phase": "advanced", "prerequisites": ["transformers_arch","computer_vision"],
        "resources": [
            {"title": "CLIP Paper (arXiv)", "url": "https://arxiv.org/abs/2103.00020"},
            {"title": "LLaVA Repository", "url": "https://github.com/haotian-liu/LLaVA"},
        ],
        "why": "Multimodal models combining vision and language are the frontier of AI research.",
        "tracks": ["ai_research"],
        "salary_impact": "+$25k",
    },
    "bayesian_ml": {
        "hours": 40, "phase": "advanced", "prerequisites": ["probability","deep_learning"],
        "resources": [
            {"title": "Bayesian Methods for Hackers (free)", "url": "https://github.com/CamDavidsonPilon/Probabilistic-Programming-and-Bayesian-Methods-for-Hackers"},
            {"title": "PyMC Documentation", "url": "https://www.pymc.io/welcome.html"},
        ],
        "why": "Bayesian methods provide uncertainty quantification - critical in healthcare and science ML.",
        "tracks": ["ai_research","data_science"],
        "salary_impact": "+$15k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # DATA SCIENCE TRACK
    # ═══════════════════════════════════════════════════════════════════════════
    "data_visualization": {
        "hours": 25, "phase": "core", "prerequisites": ["pandas"],
        "resources": [
            {"title": "Plotly Python Guide", "url": "https://plotly.com/python/"},
            {"title": "Tableau Public (free)", "url": "https://public.tableau.com/"},
            {"title": "Observable Plot", "url": "https://observablehq.com/plot/"},
        ],
        "why": "Data visualization turns numbers into decisions - the primary deliverable of a data scientist.",
        "tracks": ["data_science"],
        "salary_impact": "+$8k",
    },
    "data_wrangling": {
        "hours": 30, "phase": "core", "prerequisites": ["pandas","sql"],
        "resources": [
            {"title": "Kaggle Data Cleaning Course", "url": "https://www.kaggle.com/learn/data-cleaning"},
            {"title": "Tidy Data Paper (free)", "url": "https://vita.had.co.nz/papers/tidy-data.pdf"},
        ],
        "why": "80% of data science work is cleaning and reshaping data.",
        "tracks": ["data_science"],
        "salary_impact": "+$5k",
    },
    "apache_spark": {
        "hours": 45, "phase": "specialization", "prerequisites": ["python","sql"],
        "resources": [
            {"title": "Databricks Free Training", "url": "https://www.databricks.com/learn/training/free"},
            {"title": "Spark Official Docs", "url": "https://spark.apache.org/docs/latest/"},
            {"title": "Learning Spark (free preview)", "url": "https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf"},
        ],
        "why": "Spark is the industry standard for processing data at petabyte scale.",
        "tracks": ["data_science"],
        "salary_impact": "+$15k",
    },
    "dbt": {
        "hours": 20, "phase": "specialization", "prerequisites": ["sql"],
        "resources": [
            {"title": "dbt Learn (free)", "url": "https://learn.getdbt.com/"},
            {"title": "dbt Best Practices", "url": "https://docs.getdbt.com/guides/best-practices"},
        ],
        "why": "dbt is the modern standard for SQL-based data transformation in the data warehouse.",
        "tracks": ["data_science"],
        "salary_impact": "+$10k",
    },
    "airflow": {
        "hours": 30, "phase": "specialization", "prerequisites": ["python","sql","docker"],
        "resources": [
            {"title": "Apache Airflow Docs", "url": "https://airflow.apache.org/docs/apache-airflow/stable/index.html"},
            {"title": "Astronomer Academy (free)", "url": "https://academy.astronomer.io/"},
            {"title": "Airflow Quick Start", "url": "https://airflow.apache.org/docs/apache-airflow/stable/start.html"},
        ],
        "why": "Airflow is the standard workflow orchestrator for data pipelines and ETL at scale.",
        "tracks": ["data_science","data_engineering","ml"],
        "salary_impact": "+$18k",
    },
    "storytelling": {
        "hours": 20, "phase": "advanced", "prerequisites": ["data_visualization"],
        "resources": [
            {"title": "Storytelling with Data (book)", "url": "https://www.storytellingwithdata.com/"},
            {"title": "Cole Nussbaumer Knaflic Blog", "url": "https://www.storytellingwithdata.com/blog"},
        ],
        "why": "Influencing decisions with data requires narrative, not just charts.",
        "tracks": ["data_science"],
        "salary_impact": "+$8k",
    },
    "ab_testing": {
        "hours": 25, "phase": "specialization", "prerequisites": ["statistics","python"],
        "resources": [
            {"title": "Evan Miller A/B Testing Tools", "url": "https://www.evanmiller.org/ab-testing/"},
            {"title": "Trustworthy Online Experiments (book)", "url": "https://experimentguide.com/"},
        ],
        "why": "A/B testing is how data-driven companies make product decisions - core data science skill.",
        "tracks": ["data_science"],
        "salary_impact": "+$10k",
    },
    "data_warehouse": {
        "hours": 30, "phase": "specialization", "prerequisites": ["sql","dbt"],
        "resources": [
            {"title": "Snowflake Getting Started", "url": "https://docs.snowflake.com/en/user-guide-getting-started"},
            {"title": "BigQuery Documentation", "url": "https://cloud.google.com/bigquery/docs"},
        ],
        "why": "Modern data stacks are built on cloud data warehouses - Snowflake, BigQuery, Redshift.",
        "tracks": ["data_science"],
        "salary_impact": "+$12k",
    },
    "streaming_data": {
        "hours": 30, "phase": "advanced", "prerequisites": ["apache_spark"],
        "resources": [
            {"title": "Kafka Quickstart", "url": "https://kafka.apache.org/quickstart"},
            {"title": "Confluent Kafka Training (free)", "url": "https://developer.confluent.io/learn-kafka/"},
        ],
        "why": "Real-time data pipelines require streaming processing skills.",
        "tracks": ["data_science"],
        "salary_impact": "+$14k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # BACKEND DEVELOPER TRACK
    # ═══════════════════════════════════════════════════════════════════════════
    "fastapi_fw": {
        "hours": 30, "phase": "core", "prerequisites": ["python"],
        "resources": [
            {"title": "FastAPI Official Docs", "url": "https://fastapi.tiangolo.com/"},
            {"title": "Full FastAPI Course (YouTube)", "url": "https://www.youtube.com/watch?v=0sOvCWFmrtA"},
        ],
        "why": "FastAPI is the fastest-growing Python web framework and dominant in ML-serving APIs.",
        "tracks": ["backend","ml"],
        "salary_impact": "+$8k",
    },
    "django": {
        "hours": 50, "phase": "core", "prerequisites": ["python"],
        "resources": [
            {"title": "Django Official Tutorial", "url": "https://docs.djangoproject.com/en/5.0/intro/tutorial01/"},
            {"title": "Django for Beginners (free)", "url": "https://djangoforbeginners.com/"},
        ],
        "why": "Django is the batteries-included framework for building complex production web applications.",
        "tracks": ["backend"],
        "salary_impact": "+$10k",
    },
    "postgresql": {
        "hours": 35, "phase": "core", "prerequisites": ["sql"],
        "resources": [
            {"title": "PostgreSQL Tutorial", "url": "https://www.postgresqltutorial.com/"},
            {"title": "PgExercises", "url": "https://pgexercises.com/"},
            {"title": "Use the Index, Luke (free)", "url": "https://use-the-index-luke.com/"},
        ],
        "why": "PostgreSQL is the world's most advanced open-source RDBMS - used in every scale company.",
        "tracks": ["backend"],
        "salary_impact": "+$10k",
    },
    "rest_api_design": {
        "hours": 20, "phase": "core", "prerequisites": ["fastapi_fw"],
        "resources": [
            {"title": "REST API Design Best Practices", "url": "https://restfulapi.net/"},
            {"title": "Microsoft REST API Guidelines", "url": "https://github.com/microsoft/api-guidelines"},
        ],
        "why": "Well-designed APIs define the contracts that teams depend on at scale.",
        "tracks": ["backend"],
        "salary_impact": "+$5k",
    },
    "microservices": {
        "hours": 40, "phase": "specialization", "prerequisites": ["rest_api_design","docker"],
        "resources": [
            {"title": "Microservices.io Patterns", "url": "https://microservices.io/patterns/index.html"},
            {"title": "Microservices.io", "url": "https://microservices.io/"},
        ],
        "why": "Microservices architecture is the standard for scalable backend systems.",
        "tracks": ["backend"],
        "salary_impact": "+$12k",
    },
    "golang": {
        "hours": 60, "phase": "specialization", "prerequisites": ["data_structures_algorithms"],
        "resources": [
            {"title": "Tour of Go", "url": "https://go.dev/tour/welcome/1"},
            {"title": "Go by Example", "url": "https://gobyexample.com/"},
            {"title": "Effective Go", "url": "https://go.dev/doc/effective_go"},
        ],
        "why": "Go's performance and simplicity make it the preferred language for high-throughput backend systems.",
        "tracks": ["backend"],
        "salary_impact": "+$18k",
    },
    "caching_systems": {
        "hours": 20, "phase": "specialization", "prerequisites": ["postgresql"],
        "resources": [
            {"title": "Redis University (free)", "url": "https://university.redis.com/"},
            {"title": "Redis in Action", "url": "https://redis.io/docs/get-started/"},
        ],
        "why": "Caching is essential for application performance at any meaningful scale.",
        "tracks": ["backend"],
        "salary_impact": "+$6k",
    },
    "system_design": {
        "hours": 50, "phase": "advanced", "prerequisites": ["microservices","postgresql"],
        "resources": [
            {"title": "System Design Primer (GitHub)", "url": "https://github.com/donnemartin/system-design-primer"},
            {"title": "ByteByteGo Newsletter", "url": "https://bytebytego.com/"},
            {"title": "Designing Data-Intensive Applications", "url": "https://dataintensive.net/"},
        ],
        "why": "System design ability determines whether you can operate at senior / staff engineer level.",
        "tracks": ["backend"],
        "salary_impact": "+$28k",
    },
    "grpc_protobuf": {
        "hours": 20, "phase": "specialization", "prerequisites": ["rest_api_design"],
        "resources": [
            {"title": "gRPC Official Docs", "url": "https://grpc.io/docs/"},
            {"title": "Protocol Buffers Guide", "url": "https://protobuf.dev/getting-started/pythontutorial/"},
        ],
        "why": "gRPC is the standard for high-performance microservice communication.",
        "tracks": ["backend"],
        "salary_impact": "+$8k",
    },
    "software_testing": {
        "hours": 25, "phase": "core", "prerequisites": ["python"],
        "resources": [
            {"title": "pytest Documentation", "url": "https://docs.pytest.org/"},
            {"title": "Python Testing with pytest", "url": "https://pragprog.com/titles/bopytest2/python-testing-with-pytest-second-edition/"},
            {"title": "Obey the Testing Goat", "url": "https://www.obeythetestinggoat.com/"},
        ],
        "why": "Automated testing is non-negotiable at production engineering teams.",
        "tracks": ["ml","backend","frontend"],
        "salary_impact": "+$6k",
    },
    "message_queues": {
        "hours": 25, "phase": "specialization", "prerequisites": ["microservices"],
        "resources": [
            {"title": "RabbitMQ Tutorials", "url": "https://www.rabbitmq.com/tutorials"},
            {"title": "Kafka for Beginners", "url": "https://developer.confluent.io/learn-kafka/apache-kafka/events/"},
        ],
        "why": "Async messaging with queues enables decoupled, resilient distributed systems.",
        "tracks": ["backend"],
        "salary_impact": "+$10k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # FRONTEND DEVELOPER TRACK
    # ═══════════════════════════════════════════════════════════════════════════
    "react": {
        "hours": 60, "phase": "core", "prerequisites": ["javascript","html_css"],
        "resources": [
            {"title": "React Official Docs (react.dev)", "url": "https://react.dev/learn"},
            {"title": "Full Stack Open (free)", "url": "https://fullstackopen.com/en/"},
            {"title": "The Joy of Code", "url": "https://joyofcode.xyz/"},
        ],
        "why": "React is the dominant framework for building production UIs - used at Meta, Airbnb, Vercel.",
        "tracks": ["frontend"],
        "salary_impact": "+$16k",
    },
    "nextjs": {
        "hours": 40, "phase": "specialization", "prerequisites": ["react","typescript"],
        "resources": [
            {"title": "Next.js Official Learn", "url": "https://nextjs.org/learn"},
            {"title": "Next.js Documentation", "url": "https://nextjs.org/docs"},
        ],
        "why": "Next.js is the production-standard React framework for web applications.",
        "tracks": ["frontend"],
        "salary_impact": "+$12k",
    },
    "tailwind_css": {
        "hours": 15, "phase": "core", "prerequisites": ["html_css"],
        "resources": [
            {"title": "Tailwind CSS Docs", "url": "https://tailwindcss.com/docs"},
            {"title": "Tailwind UI Playground", "url": "https://play.tailwindcss.com/"},
        ],
        "why": "Tailwind enables rapid, consistent UI development - standard in the React ecosystem.",
        "tracks": ["frontend"],
        "salary_impact": "+$5k",
    },
    "web_performance": {
        "hours": 25, "phase": "specialization", "prerequisites": ["react"],
        "resources": [
            {"title": "web.dev Performance", "url": "https://web.dev/performance/"},
            {"title": "Chrome DevTools Profiling", "url": "https://developer.chrome.com/docs/devtools/"},
        ],
        "why": "Performance directly impacts user experience, SEO, and business metrics.",
        "tracks": ["frontend"],
        "salary_impact": "+$8k",
    },
    "graphql": {
        "hours": 25, "phase": "specialization", "prerequisites": ["react","rest_api_design"],
        "resources": [
            {"title": "How to GraphQL (free)", "url": "https://www.howtographql.com/"},
            {"title": "Apollo GraphQL Docs", "url": "https://www.apollographql.com/docs/"},
        ],
        "why": "GraphQL is standard in modern product companies - Shopify, GitHub, Twitter all use it.",
        "tracks": ["frontend"],
        "salary_impact": "+$8k",
    },
    "testing_frontend": {
        "hours": 25, "phase": "advanced", "prerequisites": ["react"],
        "resources": [
            {"title": "Testing Library", "url": "https://testing-library.com/docs/react-testing-library/intro/"},
            {"title": "Playwright Docs", "url": "https://playwright.dev/docs/intro"},
            {"title": "Vitest", "url": "https://vitest.dev/guide/"},
        ],
        "why": "Automated testing prevents regressions - a professional requirement in any team.",
        "tracks": ["frontend"],
        "salary_impact": "+$6k",
    },
    "state_management": {
        "hours": 20, "phase": "specialization", "prerequisites": ["react"],
        "resources": [
            {"title": "Redux Toolkit Docs", "url": "https://redux-toolkit.js.org/introduction/getting-started"},
            {"title": "Zustand", "url": "https://docs.pmnd.rs/zustand/getting-started/introduction"},
            {"title": "Jotai", "url": "https://jotai.org/docs/introduction"},
        ],
        "why": "Complex UIs require predictable state management patterns.",
        "tracks": ["frontend"],
        "salary_impact": "+$5k",
    },
    "web_accessibility": {
        "hours": 20, "phase": "specialization", "prerequisites": ["html_css","react"],
        "resources": [
            {"title": "MDN Accessibility Guide", "url": "https://developer.mozilla.org/en-US/docs/Web/Accessibility"},
            {"title": "a11y Project", "url": "https://www.a11yproject.com/"},
        ],
        "why": "Accessibility is a legal requirement in many countries and a mark of engineering quality.",
        "tracks": ["frontend"],
        "salary_impact": "+$4k",
    },
    "build_tooling": {
        "hours": 15, "phase": "core", "prerequisites": ["javascript"],
        "resources": [
            {"title": "Vite Documentation", "url": "https://vitejs.dev/guide/"},
            {"title": "Webpack Concepts", "url": "https://webpack.js.org/concepts/"},
        ],
        "why": "Understanding bundlers and build tools is essential for optimising frontend delivery.",
        "tracks": ["frontend"],
        "salary_impact": "+$4k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # DEVOPS TRACK
    # ═══════════════════════════════════════════════════════════════════════════
    "docker": {
        "hours": 30, "phase": "foundation", "prerequisites": ["linux"],
        "resources": [
            {"title": "Docker Getting Started", "url": "https://docs.docker.com/get-started/"},
            {"title": "Docker & Kubernetes Full Course (YouTube)", "url": "https://www.youtube.com/watch?v=bhBSlnQcq2k"},
            {"title": "Play with Docker (browser)", "url": "https://labs.play-with-docker.com/"},
        ],
        "why": "Docker containerisation is ubiquitous - every modern deployment pipeline uses it.",
        "tracks": ["ml","backend","devops"],
        "salary_impact": "+$10k",
    },
    "kubernetes": {
        "hours": 60, "phase": "specialization", "prerequisites": ["docker"],
        "resources": [
            {"title": "Kubernetes Official Tutorial", "url": "https://kubernetes.io/docs/tutorials/"},
            {"title": "KodeKloud Free Courses", "url": "https://kodekloud.com/"},
            {"title": "CKAD Exercises (GitHub)", "url": "https://github.com/dgkanatsios/CKAD-exercises"},
            {"title": "Play with Kubernetes", "url": "https://labs.play-with-k8s.com/"},
        ],
        "why": "Kubernetes is the industry standard for container orchestration at scale.",
        "tracks": ["devops"],
        "salary_impact": "+$20k",
    },
    "terraform": {
        "hours": 35, "phase": "specialization", "prerequisites": ["aws"],
        "resources": [
            {"title": "HashiCorp Learn Terraform (free)", "url": "https://developer.hashicorp.com/terraform/tutorials"},
            {"title": "Terraform Up & Running", "url": "https://www.terraformupandrunning.com/"},
        ],
        "why": "Terraform is the dominant IaC tool - infrastructure as code across all cloud providers.",
        "tracks": ["devops"],
        "salary_impact": "+$15k",
    },
    "aws": {
        "hours": 60, "phase": "core", "prerequisites": ["linux"],
        "resources": [
            {"title": "AWS Skill Builder (free)", "url": "https://skillbuilder.aws/"},
            {"title": "AWS Cloud Practitioner Essentials (free)", "url": "https://explore.skillbuilder.aws/learn/course/external/view/elearning/134/aws-cloud-practitioner-essentials"},
            {"title": "A Cloud Guru AWS Intro", "url": "https://acloudguru.com/course/introduction-to-aws"},
        ],
        "why": "AWS is the leading cloud platform with 33% market share - most deployments run here.",
        "tracks": ["devops","ml","backend"],
        "salary_impact": "+$20k",
    },
    "ci_cd": {
        "hours": 30, "phase": "specialization", "prerequisites": ["git","docker"],
        "resources": [
            {"title": "GitHub Actions Docs", "url": "https://docs.github.com/en/actions"},
            {"title": "GitLab CI/CD Guide", "url": "https://docs.gitlab.com/ee/ci/"},
            {"title": "Jenkins Getting Started", "url": "https://www.jenkins.io/doc/pipeline/tour/getting-started/"},
        ],
        "why": "CI/CD automates testing and deployment - the foundation of DevOps culture.",
        "tracks": ["devops"],
        "salary_impact": "+$12k",
    },
    "ansible": {
        "hours": 25, "phase": "specialization", "prerequisites": ["linux"],
        "resources": [
            {"title": "Ansible Getting Started", "url": "https://docs.ansible.com/ansible/latest/getting_started/index.html"},
            {"title": "Ansible for DevOps (book)", "url": "https://www.ansiblefordevops.com/"},
        ],
        "why": "Ansible automates configuration management across fleets of servers.",
        "tracks": ["devops"],
        "salary_impact": "+$8k",
    },
    "prometheus_grafana": {
        "hours": 25, "phase": "advanced", "prerequisites": ["kubernetes"],
        "resources": [
            {"title": "Prometheus Official Docs", "url": "https://prometheus.io/docs/introduction/overview/"},
            {"title": "Grafana Tutorials (free)", "url": "https://grafana.com/tutorials/"},
            {"title": "Grafana Play (browser demo)", "url": "https://play.grafana.org/"},
        ],
        "why": "You can't operate what you can't observe - monitoring is critical for reliability.",
        "tracks": ["devops"],
        "salary_impact": "+$10k",
    },
    "site_reliability": {
        "hours": 40, "phase": "advanced", "prerequisites": ["prometheus_grafana","kubernetes"],
        "resources": [
            {"title": "Google SRE Book (free)", "url": "https://sre.google/sre-book/table-of-contents/"},
            {"title": "Google SRE Workbook (free)", "url": "https://sre.google/workbook/table-of-contents/"},
        ],
        "why": "SRE practices define how world-class teams maintain reliability at scale.",
        "tracks": ["devops"],
        "salary_impact": "+$22k",
    },
    "cloud_architecture": {
        "hours": 40, "phase": "advanced", "prerequisites": ["aws"],
        "resources": [
            {"title": "AWS Well-Architected Framework", "url": "https://aws.amazon.com/architecture/well-architected/"},
            {"title": "AWS Architecture Center", "url": "https://aws.amazon.com/architecture/"},
        ],
        "why": "Cloud architecture determines cost, scale, and reliability of production systems.",
        "tracks": ["devops","ml","backend"],
        "salary_impact": "+$18k",
    },
    "helm": {
        "hours": 20, "phase": "specialization", "prerequisites": ["kubernetes"],
        "resources": [
            {"title": "Helm Documentation", "url": "https://helm.sh/docs/"},
            {"title": "Helm Chart Tutorial", "url": "https://helm.sh/docs/chart_template_guide/"},
        ],
        "why": "Helm is the package manager for Kubernetes - standard for deploying applications.",
        "tracks": ["devops"],
        "salary_impact": "+$8k",
    },
    "service_mesh": {
        "hours": 25, "phase": "advanced", "prerequisites": ["kubernetes"],
        "resources": [
            {"title": "Istio Documentation", "url": "https://istio.io/latest/docs/"},
            {"title": "Linkerd Getting Started", "url": "https://linkerd.io/2.15/getting-started/"},
        ],
        "why": "Service meshes manage traffic, security, and observability in microservice architectures.",
        "tracks": ["devops"],
        "salary_impact": "+$12k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # CYBERSECURITY TRACK
    # ═══════════════════════════════════════════════════════════════════════════
    "networking_fundamentals": {
        "hours": 40, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Professor Messer CompTIA Network+ (free)", "url": "https://www.professormesser.com/network-plus/n10-008/n10-008-video/n10-008-training-course/"},
            {"title": "Cisco Networking Academy (free)", "url": "https://www.netacad.com/"},
            {"title": "Julia Evans Zines (networking)", "url": "https://wizardzines.com/"},
        ],
        "why": "TCP/IP, DNS, and routing knowledge is the foundation of all security analysis.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$8k",
    },
    "cryptography": {
        "hours": 35, "phase": "core", "prerequisites": ["networking_fundamentals"],
        "resources": [
            {"title": "Crypto 101", "url": "https://www.crypto101.io/"},
            {"title": "Crypto101 (free book)", "url": "https://www.crypto101.io/"},
            {"title": "CryptoPals Challenges", "url": "https://cryptopals.com/"},
        ],
        "why": "Cryptography is the mathematical foundation of all secure communications.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$10k",
    },
    "penetration_testing": {
        "hours": 80, "phase": "specialization", "prerequisites": ["networking_fundamentals","linux"],
        "resources": [
            {"title": "picoCTF", "url": "https://picoctf.org/"},
            {"title": "Hack The Box", "url": "https://www.hackthebox.com/"},
            {"title": "PortSwigger Web Security Academy (free)", "url": "https://portswigger.net/web-security"},
            {"title": "OWASP Testing Guide (free)", "url": "https://owasp.org/www-project-web-security-testing-guide/"},
        ],
        "why": "Hands-on penetration testing is the highest-demand offensive security skill.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$22k",
    },
    "siem_tools": {
        "hours": 30, "phase": "specialization", "prerequisites": ["networking_fundamentals"],
        "resources": [
            {"title": "Splunk Free Training", "url": "https://www.splunk.com/en_us/training/free-courses.html"},
            {"title": "Elastic SIEM Guide", "url": "https://www.elastic.co/guide/en/security/current/es-overview.html"},
        ],
        "why": "SIEM platforms are the primary toolset of Security Operations Center analysts.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$12k",
    },
    "secure_coding": {
        "hours": 30, "phase": "core", "prerequisites": ["python"],
        "resources": [
            {"title": "OWASP Top 10", "url": "https://owasp.org/www-project-top-ten/"},
            {"title": "OWASP WebGoat (practice)", "url": "https://owasp.org/www-project-webgoat/"},
            {"title": "Secure Code Warrior", "url": "https://www.securecodewarrior.com/"},
        ],
        "why": "Understanding vulnerabilities from the developer side prevents them at the source.",
        "tracks": ["cybersecurity","backend"],
        "salary_impact": "+$10k",
    },
    "incident_response": {
        "hours": 30, "phase": "advanced", "prerequisites": ["siem_tools"],
        "resources": [
            {"title": "SANS Incident Response Cheat Sheets (free)", "url": "https://www.sans.org/blog/the-ultimate-list-of-sans-cheat-sheets/"},
            {"title": "NIST Computer Security Incident Handling Guide", "url": "https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-61r2.pdf"},
        ],
        "why": "Incident response is the core function of every security operations team.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$12k",
    },
    "zero_trust": {
        "hours": 20, "phase": "advanced", "prerequisites": ["networking_fundamentals","cryptography"],
        "resources": [
            {"title": "NIST Zero Trust Architecture (free)", "url": "https://csrc.nist.gov/publications/detail/sp/800-207/final"},
            {"title": "Google BeyondCorp", "url": "https://cloud.google.com/beyondcorp"},
        ],
        "why": "Zero trust is the modern security model for distributed cloud-first organisations.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$14k",
    },
    "malware_analysis": {
        "hours": 40, "phase": "advanced", "prerequisites": ["penetration_testing"],
        "resources": [
            {"title": "ANY.RUN Interactive Sandbox", "url": "https://any.run/"},
            {"title": "Practical Malware Analysis (book)", "url": "https://nostarch.com/malware"},
            {"title": "Malware Unicorn Workshops (free)", "url": "https://malwareunicorn.org/#/workshops"},
        ],
        "why": "Malware analysis underpins threat intelligence, incident response, and reverse engineering.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$18k",
    },
    "cloud_security": {
        "hours": 35, "phase": "advanced", "prerequisites": ["aws","zero_trust"],
        "resources": [
            {"title": "AWS Security Learning Plan (free)", "url": "https://explore.skillbuilder.aws/learn/public/learning_plan/view/97/security-learning-plan"},
            {"title": "Cloud Security Alliance", "url": "https://cloudsecurityalliance.org/research/"},
        ],
        "why": "Cloud security is the fastest-growing specialisation as infrastructure migrates to cloud.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$20k",
    },
    "threat_intelligence": {
        "hours": 25, "phase": "advanced", "prerequisites": ["incident_response"],
        "resources": [
            {"title": "MITRE ATT&CK Framework (free)", "url": "https://attack.mitre.org/"},
            {"title": "OpenCTI Documentation", "url": "https://docs.opencti.io/latest/"},
        ],
        "why": "Threat intelligence drives proactive defence - understanding adversary tactics and techniques.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$15k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # BIOINFORMATICS TRACK
    # ═══════════════════════════════════════════════════════════════════════════
    "r_programming": {
        "hours": 50, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "R for Data Science (free)", "url": "https://r4ds.hadley.nz/"},
            {"title": "Bioconductor Workflows (free)", "url": "https://www.bioconductor.org/packages/release/BiocViews.html#___Workflow"},
            {"title": "swirl (learn R interactively)", "url": "https://swirlstats.com/"},
        ],
        "why": "R is the primary language for bioinformatics, genomics, and computational biology.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$10k",
    },
    "molecular_biology": {
        "hours": 40, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Khan Academy Biology (free)", "url": "https://www.khanacademy.org/science/ap-biology"},
            {"title": "MIT 7.01 OCW (free)", "url": "https://ocw.mit.edu/courses/7-01sc-fundamentals-of-biology-fall-2011/"},
            {"title": "iBiology (free video lectures)", "url": "https://www.ibiology.org/"},
        ],
        "why": "Domain knowledge in molecular biology is essential for interpreting genomic data correctly.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$6k",
    },
    "sequence_analysis": {
        "hours": 45, "phase": "core", "prerequisites": ["python","r_programming","molecular_biology"],
        "resources": [
            {"title": "Rosalind Bioinformatics Problems (free)", "url": "https://rosalind.info/problems/locations/"},
            {"title": "UCSC Genome Browser", "url": "https://genome.ucsc.edu/"},
            {"title": "NCBI Tutorial (free)", "url": "https://www.ncbi.nlm.nih.gov/home/learn/"},
        ],
        "why": "Sequence analysis is the foundation of all computational genomics work.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$12k",
    },
    "ngs_pipelines": {
        "hours": 50, "phase": "specialization", "prerequisites": ["sequence_analysis","linux"],
        "resources": [
            {"title": "Nextflow Documentation", "url": "https://www.nextflow.io/docs/latest/index.html"},
            {"title": "GATK Best Practices", "url": "https://gatk.broadinstitute.org/hc/en-us"},
            {"title": "nf-core Pipelines", "url": "https://nf-co.re/"},
        ],
        "why": "NGS pipelines process raw sequencing data - the core of modern genomics.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$16k",
    },
    "single_cell_analysis": {
        "hours": 50, "phase": "specialization", "prerequisites": ["r_programming","sequence_analysis"],
        "resources": [
            {"title": "Seurat Tutorial (free)", "url": "https://satijalab.org/seurat/articles/get_started.html"},
            {"title": "Scanpy Tutorial (free)", "url": "https://scanpy-tutorials.readthedocs.io/"},
            {"title": "Orchestrating Single-Cell Analysis (free book)", "url": "https://bioconductor.org/books/release/OSCA/"},
        ],
        "why": "Single-cell analysis is a rapidly expanding field in precision medicine and cancer research.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$18k",
    },
    "structural_bioinformatics": {
        "hours": 45, "phase": "specialization", "prerequisites": ["molecular_biology","python"],
        "resources": [
            {"title": "RCSB PDB Training (free)", "url": "https://www.rcsb.org/pages/training"},
            {"title": "PyMOL Wiki", "url": "https://pymolwiki.org/index.php/Main_Page"},
            {"title": "AlphaFold Colab Notebook", "url": "https://colab.research.google.com/github/deepmind/alphafold/blob/main/notebooks/AlphaFold.ipynb"},
        ],
        "why": "Protein structure analysis drives drug discovery - AlphaFold has transformed this field.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$16k",
    },
    "metagenomics": {
        "hours": 40, "phase": "advanced", "prerequisites": ["ngs_pipelines"],
        "resources": [
            {"title": "QIIME 2 Documentation (free)", "url": "https://docs.qiime2.org/"},
            {"title": "MGnify Tutorial", "url": "https://www.ebi.ac.uk/training/online/courses/mgnify-quick-tour/"},
        ],
        "why": "Metagenomics enables microbiome research - a high-growth area in pharma and agriculture.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$14k",
    },
    "biopython": {
        "hours": 25, "phase": "core", "prerequisites": ["python","sequence_analysis"],
        "resources": [
            {"title": "Biopython Tutorial (free)", "url": "https://biopython.org/DIST/docs/tutorial/Tutorial.html"},
            {"title": "Biopython Cookbook", "url": "https://biopython.org/wiki/Cookbook"},
        ],
        "why": "BioPython provides the tools for sequence parsing, database access, and structure analysis.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$8k",
    },
    "genomics_databases": {
        "hours": 20, "phase": "core", "prerequisites": ["sequence_analysis"],
        "resources": [
            {"title": "ENSEMBL Tutorial", "url": "https://www.ensembl.org/info/website/tutorials/index.html"},
            {"title": "NCBI Databases (free)", "url": "https://www.ncbi.nlm.nih.gov/"},
        ],
        "why": "Knowing the major genomics databases is essential for sourcing and annotating data.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$5k",
    },
    "clinical_bioinformatics": {
        "hours": 35, "phase": "advanced", "prerequisites": ["ngs_pipelines","statistics"],
        "resources": [
            {"title": "ACMG Guidelines", "url": "https://www.acmg.net/ACMG/Medical-Genetics-Practice-Resources/Practice-Guidelines.aspx"},
            {"title": "ClinVar Database", "url": "https://www.ncbi.nlm.nih.gov/clinvar/"},
        ],
        "why": "Clinical bioinformatics bridges research and patient care in diagnostic genomics.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$20k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # SHARED ADVANCED / CROSS-TRACK
    # ═══════════════════════════════════════════════════════════════════════════
    "agile_scrum": {
        "hours": 10, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Scrum Guide (free)", "url": "https://scrumguides.org/scrum-guide.html"},
            {"title": "Atlassian Agile Coach (free)", "url": "https://www.atlassian.com/agile"},
        ],
        "why": "Virtually every engineering team works in Agile sprints.",
        "tracks": ["backend","frontend","devops"],
        "salary_impact": "+$3k",
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # NEW SKILLS - v3 additions (150+ graph completion)
    # ═══════════════════════════════════════════════════════════════════════════
    "rust": {
        "hours": 70, "phase": "specialization", "prerequisites": ["data_structures_algorithms"],
        "resources": [
            {"title": "The Rust Book (free)", "url": "https://doc.rust-lang.org/book/"},
            {"title": "Rustlings Exercises", "url": "https://github.com/rust-lang/rustlings"},
            {"title": "Rust by Example", "url": "https://doc.rust-lang.org/rust-by-example/"},
        ],
        "why": "Rust offers C-level performance with memory safety - dominant in systems, WASM, and blockchain.",
        "tracks": ["backend","cybersecurity"],
        "salary_impact": "+$22k",
    },
    "wasm": {
        "hours": 30, "phase": "advanced", "prerequisites": ["rust","javascript"],
        "resources": [
            {"title": "WebAssembly MDN Guide", "url": "https://developer.mozilla.org/en-US/docs/WebAssembly"},
            {"title": "wasm-bindgen Guide", "url": "https://rustwasm.github.io/docs/wasm-bindgen/"},
        ],
        "why": "WebAssembly brings near-native performance to browsers - critical for compute-heavy frontend apps.",
        "tracks": ["frontend","backend"],
        "salary_impact": "+$15k",
    },
    "vim_neovim": {
        "hours": 15, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Vim Adventures (game)", "url": "https://vim-adventures.com/"},
            {"title": "Neovim Getting Started", "url": "https://neovim.io/doc/user/"},
            {"title": "ThePrimeagen Vim Course", "url": "https://github.com/ThePrimeagen/vim-be-good"},
        ],
        "why": "Vim/Neovim mastery dramatically improves editing speed - beloved by senior engineers.",
        "tracks": ["backend","cybersecurity","devops","bioinformatics"],
        "salary_impact": "+$2k",
    },
    "bash_scripting": {
        "hours": 25, "phase": "foundation", "prerequisites": ["linux"],
        "resources": [
            {"title": "Bash Scripting Tutorial (free)", "url": "https://ryanstutorials.net/bash-scripting-tutorial/"},
            {"title": "ShellCheck (linter)", "url": "https://www.shellcheck.net/"},
            {"title": "Bash Reference Manual", "url": "https://www.gnu.org/software/bash/manual/bash.html"},
        ],
        "why": "Bash scripting automates everything from deployments to data pipelines.",
        "tracks": ["devops","cybersecurity","bioinformatics","backend"],
        "salary_impact": "+$5k",
    },
    "regex": {
        "hours": 10, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Regex101 (interactive)", "url": "https://regex101.com/"},
            {"title": "RegexLearn", "url": "https://regexlearn.com/"},
        ],
        "why": "Regular expressions are the universal tool for text parsing and data extraction.",
        "tracks": ["backend","cybersecurity","bioinformatics","data_science"],
        "salary_impact": "+$2k",
    },
    "web_scraping": {
        "hours": 20, "phase": "core", "prerequisites": ["python","regex"],
        "resources": [
            {"title": "Scrapy Docs", "url": "https://docs.scrapy.org/en/latest/"},
            {"title": "Playwright Python", "url": "https://playwright.dev/python/"},
            {"title": "BeautifulSoup Tutorial", "url": "https://www.crummy.com/software/BeautifulSoup/bs4/doc/"},
        ],
        "why": "Web scraping is the foundation of data collection and competitive intelligence.",
        "tracks": ["data_science","backend","cybersecurity"],
        "salary_impact": "+$5k",
    },
    "api_testing": {
        "hours": 15, "phase": "core", "prerequisites": ["rest_api_design"],
        "resources": [
            {"title": "Postman Learning Center", "url": "https://learning.postman.com/"},
            {"title": "Bruno API Client", "url": "https://www.usebruno.com/"},
            {"title": "HTTPie Docs", "url": "https://httpie.io/docs"},
        ],
        "why": "API testing ensures reliability before shipping - required in every backend team.",
        "tracks": ["backend","devops"],
        "salary_impact": "+$4k",
    },
    "load_testing": {
        "hours": 15, "phase": "specialization", "prerequisites": ["rest_api_design","docker"],
        "resources": [
            {"title": "k6 Load Testing Guide", "url": "https://k6.io/docs/"},
            {"title": "Locust Documentation", "url": "https://docs.locust.io/"},
        ],
        "why": "Load testing identifies bottlenecks before production traffic exposes them.",
        "tracks": ["backend","devops"],
        "salary_impact": "+$6k",
    },
    "database_design": {
        "hours": 30, "phase": "core", "prerequisites": ["sql"],
        "resources": [
            {"title": "Database Design (LibreTexts)", "url": "https://eng.libretexts.org/Bookshelves/Computer_Science/Databases_and_Data_Structures"},
            {"title": "CMU 15-445 DB Internals (free)", "url": "https://15445.courses.cs.cmu.edu/fall2022/"},
        ],
        "why": "Poor schema design creates technical debt that haunts products for years.",
        "tracks": ["backend","data_science"],
        "salary_impact": "+$8k",
    },
    "orm": {
        "hours": 15, "phase": "core", "prerequisites": ["database_design"],
        "resources": [
            {"title": "SQLAlchemy Tutorial", "url": "https://docs.sqlalchemy.org/en/20/tutorial/"},
            {"title": "Prisma Docs (TypeScript ORM)", "url": "https://www.prisma.io/docs"},
        ],
        "why": "ORMs are the standard interface between application code and relational databases.",
        "tracks": ["backend","frontend"],
        "salary_impact": "+$5k",
    },
    "oauth_jwt": {
        "hours": 20, "phase": "specialization", "prerequisites": ["rest_api_design","cryptography"],
        "resources": [
            {"title": "OAuth 2.0 Simplified (free)", "url": "https://www.oauth.com/"},
            {"title": "JWT.io Introduction", "url": "https://jwt.io/introduction"},
            {"title": "Auth0 Docs", "url": "https://auth0.com/docs"},
        ],
        "why": "Authentication and authorization are the security backbone of every web application.",
        "tracks": ["backend","cybersecurity"],
        "salary_impact": "+$8k",
    },
    "websockets": {
        "hours": 15, "phase": "specialization", "prerequisites": ["fastapi_fw","javascript"],
        "resources": [
            {"title": "WebSocket MDN Guide", "url": "https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API"},
            {"title": "FastAPI WebSockets", "url": "https://fastapi.tiangolo.com/advanced/websockets/"},
        ],
        "why": "WebSockets enable real-time features - chat, live dashboards, collaborative editing.",
        "tracks": ["backend","frontend"],
        "salary_impact": "+$6k",
    },
    "serverless": {
        "hours": 20, "phase": "specialization", "prerequisites": ["aws","python"],
        "resources": [
            {"title": "AWS Lambda Docs", "url": "https://docs.aws.amazon.com/lambda/"},
            {"title": "Serverless Framework", "url": "https://www.serverless.com/framework/docs"},
            {"title": "Cloudflare Workers Docs", "url": "https://developers.cloudflare.com/workers/"},
        ],
        "why": "Serverless reduces operational overhead - functions deploy in seconds at global scale.",
        "tracks": ["backend","devops"],
        "salary_impact": "+$10k",
    },
    "edge_computing": {
        "hours": 20, "phase": "advanced", "prerequisites": ["serverless"],
        "resources": [
            {"title": "Cloudflare Edge Platform", "url": "https://developers.cloudflare.com/"},
            {"title": "Vercel Edge Functions", "url": "https://vercel.com/docs/functions/edge-functions"},
        ],
        "why": "Edge computing reduces latency by running logic close to users - the future of web infrastructure.",
        "tracks": ["backend","devops","frontend"],
        "salary_impact": "+$12k",
    },
    "vector_databases": {
        "hours": 20, "phase": "specialization", "prerequisites": ["python","deep_learning"],
        "resources": [
            {"title": "Pinecone Learning Center", "url": "https://www.pinecone.io/learn/"},
            {"title": "Qdrant Documentation", "url": "https://qdrant.tech/documentation/"},
            {"title": "ChromaDB Docs", "url": "https://docs.trychroma.com/"},
        ],
        "why": "Vector databases power semantic search and RAG pipelines - the infrastructure of the LLM era.",
        "tracks": ["ml","ai_research","backend"],
        "salary_impact": "+$15k",
    },
    "prompt_engineering": {
        "hours": 15, "phase": "core", "prerequisites": ["python"],
        "resources": [
            {"title": "Anthropic Prompt Engineering Guide", "url": "https://docs.anthropic.com/en/docs/build-with-claude/prompt-engineering/overview"},
            {"title": "OpenAI Prompt Guide", "url": "https://platform.openai.com/docs/guides/prompt-engineering"},
            {"title": "DAIR.AI Prompt Engineering Guide", "url": "https://www.promptingguide.ai/"},
        ],
        "why": "Prompt engineering maximises LLM output quality - a distinct skill as models become infrastructure.",
        "tracks": ["ml","ai_research","backend"],
        "salary_impact": "+$10k",
    },
    "langchain": {
        "hours": 20, "phase": "specialization", "prerequisites": ["prompt_engineering","vector_databases"],
        "resources": [
            {"title": "LangChain Docs", "url": "https://python.langchain.com/docs/get_started/introduction"},
            {"title": "LangGraph Docs", "url": "https://langchain-ai.github.io/langgraph/"},
        ],
        "why": "LangChain/LangGraph is the dominant framework for building LLM-powered agents and pipelines.",
        "tracks": ["ml","ai_research","backend"],
        "salary_impact": "+$14k",
    },
    "spark_streaming": {
        "hours": 25, "phase": "advanced", "prerequisites": ["apache_spark","streaming_data"],
        "resources": [
            {"title": "Structured Streaming Guide", "url": "https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html"},
            {"title": "Databricks Streaming Tutorial", "url": "https://www.databricks.com/glossary/spark-streaming"},
        ],
        "why": "Spark Streaming processes continuous data at scale - essential for real-time ML pipelines.",
        "tracks": ["data_science","ml"],
        "salary_impact": "+$14k",
    },
    "delta_lake": {
        "hours": 20, "phase": "advanced", "prerequisites": ["apache_spark","dbt"],
        "resources": [
            {"title": "Delta Lake Docs", "url": "https://docs.delta.io/latest/index.html"},
            {"title": "Databricks Delta Lake Guide", "url": "https://www.databricks.com/product/delta-lake-on-databricks"},
        ],
        "why": "Delta Lake brings ACID transactions to data lakes - the foundation of the modern lakehouse.",
        "tracks": ["data_science"],
        "salary_impact": "+$12k",
    },
    "data_contracts": {
        "hours": 15, "phase": "advanced", "prerequisites": ["dbt","data_warehouse"],
        "resources": [
            {"title": "Data Contracts 101", "url": "https://dataproducts.substack.com/p/an-engineers-guide-to-data-contracts"},
            {"title": "Soda Data Contracts Docs", "url": "https://docs.soda.io/soda-cl/data-contracts.html"},
        ],
        "why": "Data contracts enforce schema agreements between teams - solving the silent data breakage problem.",
        "tracks": ["data_science"],
        "salary_impact": "+$8k",
    },
    "observability": {
        "hours": 25, "phase": "advanced", "prerequisites": ["prometheus_grafana","docker"],
        "resources": [
            {"title": "OpenTelemetry Docs", "url": "https://opentelemetry.io/docs/"},
            {"title": "Honeycomb Observability Guide", "url": "https://www.honeycomb.io/blog/so-you-want-to-build-an-observability-tool"},
            {"title": "Grafana Tempo Docs", "url": "https://grafana.com/docs/tempo/latest/"},
        ],
        "why": "Observability (traces, metrics, logs) is the modern evolution of monitoring for complex distributed systems.",
        "tracks": ["devops","backend"],
        "salary_impact": "+$12k",
    },
    "chaos_engineering": {
        "hours": 20, "phase": "advanced", "prerequisites": ["site_reliability","kubernetes"],
        "resources": [
            {"title": "Chaos Monkey (Netflix)", "url": "https://netflix.github.io/chaosmonkey/"},
            {"title": "Principles of Chaos Engineering", "url": "https://principlesofchaos.org/"},
            {"title": "LitmusChaos Docs", "url": "https://litmuschaos.io/"},
        ],
        "why": "Chaos engineering proactively uncovers resilience gaps before real failures occur.",
        "tracks": ["devops"],
        "salary_impact": "+$10k",
    },
    "supply_chain_security": {
        "hours": 20, "phase": "advanced", "prerequisites": ["secure_coding","ci_cd"],
        "resources": [
            {"title": "SLSA Framework", "url": "https://slsa.dev/"},
            {"title": "Sigstore Docs", "url": "https://docs.sigstore.dev/"},
            {"title": "SBOM Guide (CISA)", "url": "https://www.cisa.gov/sbom"},
        ],
        "why": "Software supply chain attacks (SolarWinds, Log4Shell) have made this a top security priority.",
        "tracks": ["cybersecurity","devops"],
        "salary_impact": "+$14k",
    },
    "osint": {
        "hours": 25, "phase": "specialization", "prerequisites": ["networking_fundamentals"],
        "resources": [
            {"title": "OSINT Framework", "url": "https://osintframework.com/"},
            {"title": "Trace Labs (practise)", "url": "https://www.tracelabs.org/"},
            {"title": "Bellingcat Online Investigation Toolkit", "url": "https://docs.google.com/spreadsheets/d/18rtqh8EG2q1xBo2cLNyhIDuK9jrPGwYr9DI2UncoqJQ/"},
        ],
        "why": "Open-Source Intelligence is foundational for threat intelligence, red teaming, and investigative security.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$10k",
    },
    "digital_forensics": {
        "hours": 40, "phase": "advanced", "prerequisites": ["incident_response","linux"],
        "resources": [
            {"title": "Autopsy Digital Forensics (free)", "url": "https://www.autopsy.com/"},
            {"title": "DFIR.training Resources", "url": "https://www.dfir.training/"},
            {"title": "Memory Forensics with Volatility", "url": "https://volatilityfoundation.org/"},
        ],
        "why": "Digital forensics is the investigative science behind cyber incident attribution and legal proceedings.",
        "tracks": ["cybersecurity"],
        "salary_impact": "+$15k",
    },
    "proteomics": {
        "hours": 35, "phase": "specialization", "prerequisites": ["python","molecular_biology"],
        "resources": [
            {"title": "ProteomicsDB Tutorials", "url": "https://www.proteomicsdb.org/proteomicsdb/#tutorials"},
            {"title": "UniProt Knowledge Base", "url": "https://www.uniprot.org/"},
            {"title": "Perseus Proteomics Platform", "url": "https://maxquant.net/perseus/"},
        ],
        "why": "Proteomics enables understanding of disease mechanisms at the protein level - central to drug discovery.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$14k",
    },
    "cheminformatics": {
        "hours": 40, "phase": "specialization", "prerequisites": ["python","molecular_biology"],
        "resources": [
            {"title": "RDKit Documentation", "url": "https://www.rdkit.org/docs/"},
            {"title": "DeepChem Tutorials", "url": "https://deepchem.io/tutorials/the-basic-tools-of-the-deep-life-sciences/"},
            {"title": "ChEMBL Database", "url": "https://www.ebi.ac.uk/chembl/"},
        ],
        "why": "Cheminformatics applies ML to drug-like molecules - the fastest-growing area in pharma AI.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$18k",
    },
    "phylogenetics": {
        "hours": 30, "phase": "specialization", "prerequisites": ["r_programming","sequence_analysis"],
        "resources": [
            {"title": "BEAST2 Docs", "url": "https://www.beast2.org/"},
            {"title": "IQ-TREE Tutorial", "url": "http://www.iqtree.org/doc/Tutorial"},
            {"title": "Evolution 101 (UC Berkeley)", "url": "https://evolution.berkeley.edu/"},
        ],
        "why": "Phylogenetics reconstructs evolutionary relationships - essential in pandemic tracking and biodiversity research.",
        "tracks": ["bioinformatics"],
        "salary_impact": "+$10k",
    },
    "c_plus_plus": {
        "hours": 80, "phase": "specialization", "prerequisites": ["data_structures_algorithms"],
        "resources": [
            {"title": "learncpp.com (free)", "url": "https://www.learncpp.com/"},
            {"title": "C++ Core Guidelines", "url": "https://isocpp.github.io/CppCoreGuidelines/"},
            {"title": "Compiler Explorer", "url": "https://godbolt.org/"},
        ],
        "why": "C++ is essential for performance-critical systems: game engines, HPC, embedded, and ML frameworks.",
        "tracks": ["backend","ai_research"],
        "salary_impact": "+$20k",
    },
    "java": {
        "hours": 60, "phase": "core", "prerequisites": ["data_structures_algorithms"],
        "resources": [
            {"title": "Oracle Java Tutorials (free)", "url": "https://docs.oracle.com/javase/tutorial/"},
            {"title": "Baeldung Java Guides", "url": "https://www.baeldung.com/"},
        ],
        "why": "Java powers enterprise backends, Android, and big data - one of the most in-demand languages globally.",
        "tracks": ["backend"],
        "salary_impact": "+$16k",
    },
    "scala": {
        "hours": 50, "phase": "specialization", "prerequisites": ["java","apache_spark"],
        "resources": [
            {"title": "Scala Tour", "url": "https://docs.scala-lang.org/tour/tour-of-scala.html"},
            {"title": "Scala Exercises", "url": "https://www.scala-exercises.org/"},
        ],
        "why": "Scala is the primary language of Spark and functional big-data engineering.",
        "tracks": ["data_science"],
        "salary_impact": "+$18k",
    },
    "mongodb": {
        "hours": 25, "phase": "core", "prerequisites": ["python"],
        "resources": [
            {"title": "MongoDB University (free)", "url": "https://learn.mongodb.com/"},
            {"title": "MongoDB Manual", "url": "https://www.mongodb.com/docs/manual/"},
        ],
        "why": "MongoDB is the leading NoSQL database for flexible document storage.",
        "tracks": ["backend"],
        "salary_impact": "+$7k",
    },
    "elasticsearch": {
        "hours": 25, "phase": "specialization", "prerequisites": ["python","postgresql"],
        "resources": [
            {"title": "Elasticsearch Docs", "url": "https://www.elastic.co/docs/"},
            {"title": "Elasticsearch: The Definitive Guide (free)", "url": "https://www.elastic.co/guide/en/elasticsearch/guide/current/index.html"},
        ],
        "why": "Elasticsearch powers full-text search and analytics at scale - used at most major tech companies.",
        "tracks": ["backend","data_science"],
        "salary_impact": "+$10k",
    },
    "gcp": {
        "hours": 40, "phase": "specialization", "prerequisites": ["linux","docker"],
        "resources": [
            {"title": "Google Cloud Codelabs", "url": "https://codelabs.developers.google.com/?cat=Cloud"},
            {"title": "GCP Architecture Center", "url": "https://cloud.google.com/architecture"},
        ],
        "why": "GCP leads in ML infrastructure (TPUs, Vertex AI) - essential for AI-focused cloud roles.",
        "tracks": ["ml","devops"],
        "salary_impact": "+$15k",
    },
    "azure": {
        "hours": 40, "phase": "specialization", "prerequisites": ["linux","docker"],
        "resources": [
            {"title": "Microsoft Learn (free)", "url": "https://learn.microsoft.com/en-us/training/"},
            {"title": "AZ-900 Study Guide", "url": "https://learn.microsoft.com/en-us/certifications/azure-fundamentals/"},
        ],
        "why": "Azure is dominant in enterprise - most Fortune 500 companies run workloads here.",
        "tracks": ["devops","backend"],
        "salary_impact": "+$15k",
    },
    "huggingface": {
        "hours": 25, "phase": "specialization", "prerequisites": ["pytorch","natural_language_processing"],
        "resources": [
            {"title": "HuggingFace Course (free)", "url": "https://huggingface.co/learn"},
            {"title": "HuggingFace Docs", "url": "https://huggingface.co/docs"},
        ],
        "why": "HuggingFace is the GitHub of ML models - proficiency is effectively mandatory for NLP/LLM work.",
        "tracks": ["ml","ai_research"],
        "salary_impact": "+$16k",
    },
    "dspy": {
        "hours": 20, "phase": "advanced", "prerequisites": ["prompt_engineering","langchain"],
        "resources": [
            {"title": "DSPy Documentation", "url": "https://dspy-docs.vercel.app/"},
            {"title": "DSPy GitHub", "url": "https://github.com/stanfordnlp/dspy"},
        ],
        "why": "DSPy replaces hand-crafted prompts with optimised, composable LLM programs - the next wave of AI engineering.",
        "tracks": ["ai_research","ml"],
        "salary_impact": "+$12k",
    },
    "kafka": {
        "hours": 30, "phase": "specialization", "prerequisites": ["python","docker"],
        "resources": [
            {"title": "Confluent Kafka Course (free)", "url": "https://developer.confluent.io/learn-kafka/"},
            {"title": "Kafka Quickstart", "url": "https://kafka.apache.org/quickstart"},
        ],
        "why": "Kafka is the backbone of real-time event-driven architectures at scale.",
        "tracks": ["backend","data_science","devops"],
        "salary_impact": "+$14k",
    },
    "devsecops": {
        "hours": 30, "phase": "advanced", "prerequisites": ["ci_cd","secure_coding"],
        "resources": [
            {"title": "OWASP DevSecOps Guideline", "url": "https://owasp.org/www-project-devsecops-guideline/"},
            {"title": "Snyk Learn (free)", "url": "https://learn.snyk.io/"},
        ],
        "why": "Shifting security left into the development pipeline is now an industry standard.",
        "tracks": ["devops","cybersecurity","security_intern"],
        "salary_impact": "+$15k",
    },

    # ── PRODUCT & DESIGN SKILLS ───────────────────────────────────────────────
    "product_analytics": {
        "hours": 30, "phase": "core", "prerequisites": ["sql","statistics"],
        "resources": [
            {"title": "Mixpanel Analytics Academy", "url": "https://mixpanel.com/blog/analytics-academy/"},
            {"title": "Amplitude Analytics", "url": "https://academy.amplitude.com/"},
            {"title": "Google Analytics Certification", "url": "https://skillshop.google.com/"},
        ],
        "why": "Product managers need data fluency to prioritize features and measure impact.",
        "tracks": ["product_manager"],
        "salary_impact": "+$20k",
    },
    "figma_design": {
        "hours": 40, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Figma Learn Design", "url": "https://www.figma.com/resources/learn-design/"},
            {"title": "DesignCode Figma", "url": "https://designcode.io/figma"},
            {"title": "Figma YouTube Channel", "url": "https://www.youtube.com/@Figma"},
        ],
        "why": "Figma is the industry-standard tool for UI design, prototyping, and design systems.",
        "tracks": ["ux_designer","product_manager"],
        "salary_impact": "+$12k",
    },
    "user_research": {
        "hours": 35, "phase": "core", "prerequisites": [],
        "resources": [
            {"title": "Nielsen Norman Group Articles", "url": "https://www.nngroup.com/articles/"},
            {"title": "Just Enough Research (book)", "url": "https://abookapart.com/products/just-enough-research"},
            {"title": "UX Research Field Guide", "url": "https://www.userinterviews.com/ux-research-field-guide"},
        ],
        "why": "Evidence-based design outperforms intuition — research separates great PMs/designers from average.",
        "tracks": ["ux_designer","product_manager"],
        "salary_impact": "+$15k",
    },
    "design_systems": {
        "hours": 25, "phase": "advanced", "prerequisites": ["html_css","figma_design"],
        "resources": [
            {"title": "Design Systems Handbook", "url": "https://www.designbetter.co/design-systems-handbook"},
            {"title": "Storybook.js", "url": "https://storybook.js.org/"},
            {"title": "Component Gallery", "url": "https://component.gallery/"},
        ],
        "why": "Design systems create scalable consistency and are the mark of a senior designer.",
        "tracks": ["ux_designer","frontend"],
        "salary_impact": "+$18k",
    },

    # ── MOBILE DEV SKILLS ─────────────────────────────────────────────────────
    "mobile_native": {
        "hours": 60, "phase": "advanced", "prerequisites": ["javascript","typescript"],
        "resources": [
            {"title": "React Native Docs", "url": "https://reactnative.dev/docs/getting-started"},
            {"title": "Flutter Docs", "url": "https://docs.flutter.dev/"},
            {"title": "Swift Docs (Apple)", "url": "https://swift.org/documentation/"},
        ],
        "why": "Native mobile development unlocks iOS/Android app development at platform depth.",
        "tracks": ["mobile_dev"],
        "salary_impact": "+$22k",
    },

    # ── GAME DEV SKILLS ───────────────────────────────────────────────────────
    "game_engine": {
        "hours": 80, "phase": "core", "prerequisites": ["python","linear_algebra"],
        "resources": [
            {"title": "Unity Learn", "url": "https://learn.unity.com/"},
            {"title": "Unreal Engine Docs", "url": "https://docs.unrealengine.com/"},
            {"title": "Godot Documentation", "url": "https://docs.godotengine.org/"},
        ],
        "why": "Unity and Unreal are the dominant engines — fluency here is table stakes for game dev roles.",
        "tracks": ["game_dev"],
        "salary_impact": "+$25k",
    },
    "graphics_programming": {
        "hours": 60, "phase": "advanced", "prerequisites": ["linear_algebra","calculus","game_engine"],
        "resources": [
            {"title": "Learn OpenGL", "url": "https://learnopengl.com/"},
            {"title": "GPU Gems (free online)", "url": "https://developer.nvidia.com/gpugems/gpugems/foreword"},
            {"title": "Real-Time Rendering", "url": "https://www.realtimerendering.com/"},
        ],
        "why": "Graphics programming (shaders, rendering pipelines) is a highly paid, rare skill in game dev.",
        "tracks": ["game_dev"],
        "salary_impact": "+$30k",
    },
    "physics_simulation": {
        "hours": 40, "phase": "advanced", "prerequisites": ["linear_algebra","calculus","game_engine"],
        "resources": [
            {"title": "Game Physics Tutorial", "url": "https://gafferongames.com/"},
            {"title": "The Nature of Code", "url": "https://natureofcode.com/"},
        ],
        "why": "Realistic physics simulations differentiate AAA games and are essential for immersive experiences.",
        "tracks": ["game_dev"],
        "salary_impact": "+$20k",
    },

    # ── BLOCKCHAIN SKILLS ─────────────────────────────────────────────────────
    "ethereum_solidity": {
        "hours": 60, "phase": "core", "prerequisites": ["javascript","cryptography"],
        "resources": [
            {"title": "CryptoZombies (interactive)", "url": "https://cryptozombies.io/"},
            {"title": "Solidity Docs", "url": "https://docs.soliditylang.org/"},
            {"title": "Ethereum.org Learn", "url": "https://ethereum.org/en/learn/"},
        ],
        "why": "Solidity on Ethereum is the dominant smart contract platform — most Web3 jobs require it.",
        "tracks": ["blockchain_dev"],
        "salary_impact": "+$35k",
    },
    "web3_tooling": {
        "hours": 35, "phase": "advanced", "prerequisites": ["ethereum_solidity","javascript"],
        "resources": [
            {"title": "Hardhat Docs", "url": "https://hardhat.org/docs"},
            {"title": "Foundry Book", "url": "https://book.getfoundry.sh/"},
            {"title": "ethers.js Docs", "url": "https://docs.ethers.org/"},
        ],
        "why": "Hardhat, Foundry and ethers.js are the standard toolchain for Web3 development.",
        "tracks": ["blockchain_dev"],
        "salary_impact": "+$25k",
    },
    "defi_protocols": {
        "hours": 40, "phase": "advanced", "prerequisites": ["ethereum_solidity","web3_tooling"],
        "resources": [
            {"title": "DeFi Developer Roadmap", "url": "https://github.com/OffcierCia/DeFi-Developer-Road-Map"},
            {"title": "Uniswap Docs", "url": "https://docs.uniswap.org/"},
            {"title": "Aave Docs", "url": "https://docs.aave.com/"},
        ],
        "why": "DeFi is the most active area of smart contract development with the highest salaries.",
        "tracks": ["blockchain_dev"],
        "salary_impact": "+$40k",
    },

    # ── QUANTUM SKILLS ─────────────────────────────────────────────────────────
    "quantum_algorithms": {
        "hours": 60, "phase": "core", "prerequisites": ["linear_algebra","statistics","python"],
        "resources": [
            {"title": "Quantum Computing: An Applied Approach", "url": "https://link.springer.com/book/10.1007/978-3-030-23922-0"},
            {"title": "IBM Quantum Learning", "url": "https://learning.quantum.ibm.com/"},
            {"title": "Quantum Country (spaced repetition)", "url": "https://quantum.country/"},
        ],
        "why": "Understanding Grover's, Shor's and variational algorithms is core to any QC engineering role.",
        "tracks": ["quantum_computing"],
        "salary_impact": "+$40k",
    },
    "qiskit_cirq": {
        "hours": 40, "phase": "advanced", "prerequisites": ["quantum_algorithms","python"],
        "resources": [
            {"title": "Qiskit Textbook", "url": "https://qiskit.org/learn"},
            {"title": "Cirq Docs (Google)", "url": "https://quantumai.google/cirq"},
            {"title": "PennyLane Tutorials", "url": "https://pennylane.ai/qml/"},
        ],
        "why": "Qiskit (IBM) and Cirq (Google) are the two dominant frameworks for programming quantum hardware.",
        "tracks": ["quantum_computing"],
        "salary_impact": "+$30k",
    },

    # ── DATA ENGINEERING SKILLS ────────────────────────────────────────────────
    "kafka_streaming": {
        "hours": 35, "phase": "advanced", "prerequisites": ["python","sql","docker"],
        "resources": [
            {"title": "Kafka Documentation", "url": "https://kafka.apache.org/documentation/"},
            {"title": "Confluent Developer", "url": "https://developer.confluent.io/"},
            {"title": "Kafka: The Definitive Guide (free)", "url": "https://www.confluent.io/resources/kafka-the-definitive-guide-v2/"},
        ],
        "why": "Real-time streaming with Kafka is required for event-driven architectures and ML feature stores.",
        "tracks": ["data_engineering","backend","devops"],
        "salary_impact": "+$22k",
    },
    "excel_advanced": {
        "hours": 20, "phase": "foundation", "prerequisites": [],
        "resources": [
            {"title": "Microsoft Excel Training", "url": "https://support.microsoft.com/en-us/excel"},
            {"title": "ExcelJet Functions", "url": "https://exceljet.net/"},
        ],
        "why": "Advanced Excel with pivot tables, Power Query, and VBA is required for many analyst roles.",
        "tracks": ["data_intern","data_science"],
        "salary_impact": "+$5k",
    },
}

# ── Track definitions ──────────────────────────────────────────────────────────

TRACKS: dict[str, dict] = {
    # ── INTERNSHIP TRACKS ──────────────────────────────────────────────────────
    "swe_intern": {
        "name": "Software Engineering Intern",
        "keywords": ["swe intern","software engineering intern","software engineer intern","coding intern",
                     "tech intern","engineering internship","summer intern swe","cs intern"],
        "core_skills": ["python","git","data_structures_algorithms","javascript","html_css",
                        "software_testing","sql","docker","rest_api_design"],
        "description": "Land a top software engineering internship at FAANG, startups, or unicorns.",
        "salary_range": (45000, 85000),  # annualized stipend
    },
    "ml_intern": {
        "name": "ML / AI Intern",
        "keywords": ["ml intern","ai intern","machine learning intern","data science intern",
                     "research intern ml","ml research intern","ai research intern","nlp intern"],
        "core_skills": ["python","git","statistics","numpy","pandas","scikit_learn",
                        "deep_learning","pytorch","data_wrangling","sql","experiment_tracking"],
        "description": "Break into ML/AI internships at research labs, big tech, and AI-native companies.",
        "salary_range": (50000, 95000),
    },
    "data_intern": {
        "name": "Data / Analytics Intern",
        "keywords": ["data intern","analytics intern","data analyst intern","business intelligence intern",
                     "data engineering intern","bi intern","reporting intern"],
        "core_skills": ["python","sql","pandas","data_visualization","matplotlib_seaborn",
                        "storytelling","ab_testing","data_wrangling","statistics","excel_advanced"],
        "description": "Break into data analytics and business intelligence roles at top companies.",
        "salary_range": (40000, 70000),
    },
    "devops_intern": {
        "name": "DevOps / Cloud Intern",
        "keywords": ["devops intern","cloud intern","platform intern","sre intern","infra intern",
                     "cloud engineering intern","site reliability intern"],
        "core_skills": ["linux","git","docker","python","aws","ci_cd","terraform","kubernetes"],
        "description": "Gain hands-on cloud and infrastructure experience during an internship.",
        "salary_range": (42000, 75000),
    },
    "security_intern": {
        "name": "Cybersecurity Intern",
        "keywords": ["security intern","cybersecurity intern","infosec intern","penetration testing intern",
                     "soc intern","network security intern","ethical hacking intern"],
        "core_skills": ["linux","python","networking_fundamentals","cryptography",
                        "penetration_testing","siem_tools","secure_coding"],
        "description": "Launch your cybersecurity career through internships at banks, tech firms, and agencies.",
        "salary_range": (38000, 68000),
    },
    # ── PRODUCT & DESIGN TRACKS ───────────────────────────────────────────────
    "product_manager": {
        "name": "Product Manager",
        "keywords": ["product manager","pm","product management","product lead","associate pm",
                     "apm","technical product manager","tpm","group product manager","product owner"],
        "core_skills": ["python","sql","ab_testing","storytelling","data_visualization","system_design",
                        "rest_api_design","git","statistics","product_analytics"],
        "description": "Define product strategy, prioritize features, and drive cross-functional teams.",
        "salary_range": (110000, 220000),
    },
    "ux_designer": {
        "name": "UX / Product Designer",
        "keywords": ["ux designer","ui designer","product designer","user experience designer",
                     "interaction designer","ux researcher","ui/ux","design lead","visual designer"],
        "core_skills": ["html_css","javascript","figma_design","user_research","design_systems",
                        "web_accessibility","web_performance","react","storytelling","ab_testing"],
        "description": "Design intuitive, beautiful digital experiences grounded in user research.",
        "salary_range": (95000, 190000),
    },
    # ── MOBILE DEV TRACK ──────────────────────────────────────────────────────
    "mobile_dev": {
        "name": "Mobile Developer",
        "keywords": ["mobile developer","android developer","ios developer","react native developer",
                     "flutter developer","mobile engineer","app developer","swift developer","kotlin developer"],
        "core_skills": ["javascript","typescript","react","git","html_css",
                        "data_structures_algorithms","rest_api_design","sql",
                        "software_testing","build_tooling","mobile_native"],
        "description": "Build polished iOS and Android apps used by millions of people.",
        "salary_range": (105000, 195000),
    },
    # ── GAME DEV TRACK ────────────────────────────────────────────────────────
    "game_dev": {
        "name": "Game Developer",
        "keywords": ["game developer","game engineer","unity developer","unreal developer",
                     "game programmer","gameplay engineer","graphics programmer","game dev"],
        "core_skills": ["python","javascript","git","data_structures_algorithms","linear_algebra",
                        "calculus","software_testing","game_engine","graphics_programming",
                        "physics_simulation","build_tooling"],
        "description": "Build immersive, performant games across PC, console, and mobile platforms.",
        "salary_range": (85000, 175000),
    },
    # ── BLOCKCHAIN / WEB3 TRACK ───────────────────────────────────────────────
    "blockchain_dev": {
        "name": "Blockchain / Web3 Developer",
        "keywords": ["blockchain developer","web3 developer","smart contract developer",
                     "solidity developer","defi engineer","crypto engineer","nft developer","ethereum developer"],
        "core_skills": ["javascript","typescript","git","python","cryptography",
                        "data_structures_algorithms","rest_api_design","docker",
                        "ethereum_solidity","web3_tooling","defi_protocols"],
        "description": "Build decentralized applications, protocols, and smart contracts.",
        "salary_range": (120000, 280000),
    },
    # ── QUANTUM COMPUTING TRACK ───────────────────────────────────────────────
    "quantum_computing": {
        "name": "Quantum Computing Engineer",
        "keywords": ["quantum computing","quantum engineer","quantum developer","qiskit developer",
                     "quantum researcher","quantum software","quantum algorithms"],
        "core_skills": ["python","git","linear_algebra","statistics","calculus","probability",
                        "quantum_algorithms","qiskit_cirq","numpy","research_methods","latex"],
        "description": "Program quantum circuits and algorithms for near-term quantum hardware.",
        "salary_range": (130000, 280000),
    },
    # ── DATA ENGINEERING TRACK ────────────────────────────────────────────────
    "data_engineering": {
        "name": "Data Engineer",
        "keywords": ["data engineer","data pipeline","etl developer","data infrastructure",
                     "analytics engineer","platform data","data platform engineer"],
        "core_skills": ["python","git","sql","linux","apache_spark","dbt","data_warehouse",
                        "docker","kafka_streaming","airflow","cloud_architecture","aws"],
        "description": "Build reliable data pipelines and infrastructure that power analytics.",
        "salary_range": (110000, 200000),
    },
    # ── CLOUD ARCHITECT TRACK ─────────────────────────────────────────────────
    "cloud_architect": {
        "name": "Cloud Architect",
        "keywords": ["cloud architect","solutions architect","aws architect","azure architect",
                     "gcp architect","cloud solutions engineer","enterprise architect cloud"],
        "core_skills": ["linux","git","python","docker","kubernetes","aws","terraform",
                        "ci_cd","cloud_architecture","system_design","microservices",
                        "service_mesh","caching_systems","grpc_protobuf"],
        "description": "Design multi-cloud architectures that are secure, scalable, and cost-efficient.",
        "salary_range": (140000, 280000),
    },
    # ── EXISTING TRACKS ────────────────────────────────────────────────────────
    "ml": {
        "name": "Machine Learning Engineer",
        "keywords": ["machine learning","ml","ml engineer","machine learning engineer","ml engineering","mlops","applied ml"],
        "core_skills": ["python","git","linux","statistics","linear_algebra","calculus","numpy","pandas",
                        "scikit_learn","deep_learning","pytorch","mlops","model_deployment","docker","sql",
                        "experiment_tracking","xgboost_lightgbm","feature_engineering"],
        "description": "Build, train, and deploy ML models into production systems.",
        "salary_range": (130000, 220000),
    },
    "architecture": {
        "name": "Software Architect",
        "keywords": ["architect","software architect","solution architect","system architect","technical architect",
                     "cloud architect","enterprise architect","backend architect","frontend architect",
                     "principal engineer","staff engineer","engineering lead","tech lead"],
        "core_skills": ["python","git","linux","data_structures_algorithms","sql","system_design",
                        "rest_api_design","docker","microservices","system_design","software_testing",
                        "caching_systems","grpc_protobuf","message_queues","cloud_architecture",
                        "aws","terraform","kubernetes","ci_cd"],
        "description": "Design scalable, maintainable system architectures and technical strategies.",
        "salary_range": (140000, 280000),
    },
    "ai_research": {
        "name": "AI Researcher",
        "keywords": ["ai researcher","research scientist","ai research","machine learning research",
                     "phd ai","mit computer science","ml researcher","deep learning researcher"],
        "core_skills": ["python","git","statistics","linear_algebra","calculus","probability","numpy",
                        "deep_learning","pytorch","natural_language_processing","transformers_arch",
                        "reinforcement_learning","research_methods","latex","graph_neural_networks",
                        "fine_tuning_llms","experiment_tracking"],
        "description": "Advance the frontiers of AI through original research and publication.",
        "salary_range": (140000, 300000),
    },
    "data_science": {
        "name": "Data Scientist",
        "keywords": ["data scientist","data science","data analyst","analytics","business intelligence","bi analyst"],
        "core_skills": ["python","git","statistics","sql","numpy","pandas","matplotlib_seaborn",
                        "scikit_learn","data_wrangling","data_visualization","apache_spark",
                        "feature_engineering","storytelling","ab_testing","data_warehouse",
                        "xgboost_lightgbm","dbt"],
        "description": "Extract insights from data to drive business decisions.",
        "salary_range": (100000, 180000),
    },
    "backend": {
        "name": "Backend Developer",
        "keywords": ["backend developer","backend engineer","software engineer","backend","api developer",
                     "server side developer","python developer","java developer","backend architect"],
        "core_skills": ["python","git","linux","data_structures_algorithms","sql","fastapi_fw","postgresql",
                        "rest_api_design","docker","system_design","software_testing","microservices",
                        "caching_systems","grpc_protobuf","message_queues"],
        "description": "Build scalable, reliable server-side systems and APIs.",
        "salary_range": (110000, 200000),
    },
    "frontend": {
        "name": "Frontend Developer",
        "keywords": ["frontend developer","frontend engineer","ui developer","web developer","react developer",
                     "ui engineer","web engineer","frontend architect"],
        "core_skills": ["html_css","javascript","typescript","git","react","nextjs","tailwind_css",
                        "web_performance","testing_frontend","data_structures_algorithms",
                        "state_management","web_accessibility","build_tooling"],
        "description": "Build beautiful, performant, accessible user interfaces.",
        "salary_range": (100000, 185000),
    },
    "cybersecurity": {
        "name": "Cybersecurity Analyst",
        "keywords": ["cybersecurity","security analyst","information security","infosec","penetration tester",
                     "soc analyst","security engineer","ethical hacker"],
        "core_skills": ["linux","git","python","networking_fundamentals","cryptography","penetration_testing",
                        "siem_tools","secure_coding","incident_response","zero_trust","cloud_security",
                        "threat_intelligence"],
        "description": "Protect systems and data from cyber threats and adversarial actors.",
        "salary_range": (95000, 185000),
    },
    "devops": {
        "name": "DevOps Engineer",
        "keywords": ["devops","devops engineer","sre","site reliability engineer","platform engineer",
                     "cloud engineer","infrastructure engineer","devsecops","cloud architect","infra architect"],
        "core_skills": ["linux","git","docker","kubernetes","aws","terraform","ci_cd","ansible",
                        "prometheus_grafana","python","site_reliability","helm","cloud_architecture",
                        "service_mesh"],
        "description": "Automate, scale, and secure infrastructure and delivery pipelines.",
        "salary_range": (115000, 210000),
    },
    "bioinformatics": {
        "name": "Bioinformatics Researcher",
        "keywords": ["bioinformatics","computational biology","genomics","bioinformatics researcher",
                     "genomics scientist","computational genomics"],
        "core_skills": ["python","r_programming","git","linux","statistics","molecular_biology",
                        "sequence_analysis","biopython","ngs_pipelines","single_cell_analysis",
                        "genomics_databases","structural_bioinformatics"],
        "description": "Apply computational methods to biological data - genomics, proteomics, drug discovery.",
        "salary_range": (85000, 160000),
    },
}

# ── Salary bands ───────────────────────────────────────────────────────────────

SALARY_BANDS: dict[str, list[dict]] = {
    "ml":               [{"level":"junior","low":95000,"high":135000},{"level":"mid","low":135000,"high":180000},{"level":"senior","low":180000,"high":280000}],
    "ai_research":      [{"level":"junior","low":110000,"high":150000},{"level":"mid","low":150000,"high":220000},{"level":"senior","low":220000,"high":400000}],
    "data_science":     [{"level":"junior","low":75000,"high":110000},{"level":"mid","low":110000,"high":155000},{"level":"senior","low":155000,"high":220000}],
    "backend":          [{"level":"junior","low":80000,"high":120000},{"level":"mid","low":120000,"high":165000},{"level":"senior","low":165000,"high":240000}],
    "frontend":         [{"level":"junior","low":75000,"high":110000},{"level":"mid","low":110000,"high":155000},{"level":"senior","low":155000,"high":220000}],
    "cybersecurity":    [{"level":"junior","low":70000,"high":100000},{"level":"mid","low":100000,"high":145000},{"level":"senior","low":145000,"high":210000}],
    "devops":           [{"level":"junior","low":85000,"high":120000},{"level":"mid","low":120000,"high":170000},{"level":"senior","low":170000,"high":260000}],
    "bioinformatics":   [{"level":"junior","low":60000,"high":90000},{"level":"mid","low":90000,"high":130000},{"level":"senior","low":130000,"high":185000}],
    # New tracks
    "swe_intern":       [{"level":"intern","low":38000,"high":55000},{"level":"return offer","low":80000,"high":130000},{"level":"new grad","low":100000,"high":165000}],
    "ml_intern":        [{"level":"intern","low":45000,"high":65000},{"level":"return offer","low":100000,"high":150000},{"level":"new grad","low":120000,"high":180000}],
    "data_intern":      [{"level":"intern","low":35000,"high":50000},{"level":"return offer","low":70000,"high":105000},{"level":"new grad","low":80000,"high":120000}],
    "devops_intern":    [{"level":"intern","low":38000,"high":52000},{"level":"return offer","low":85000,"high":120000},{"level":"new grad","low":95000,"high":140000}],
    "security_intern":  [{"level":"intern","low":32000,"high":48000},{"level":"return offer","low":70000,"high":100000},{"level":"new grad","low":80000,"high":115000}],
    "product_manager":  [{"level":"apm","low":95000,"high":140000},{"level":"mid","low":140000,"high":195000},{"level":"senior","low":195000,"high":320000}],
    "ux_designer":      [{"level":"junior","low":70000,"high":105000},{"level":"mid","low":105000,"high":150000},{"level":"senior","low":150000,"high":210000}],
    "mobile_dev":       [{"level":"junior","low":80000,"high":115000},{"level":"mid","low":115000,"high":160000},{"level":"senior","low":160000,"high":230000}],
    "game_dev":         [{"level":"junior","low":65000,"high":95000},{"level":"mid","low":95000,"high":140000},{"level":"senior","low":140000,"high":200000}],
    "blockchain_dev":   [{"level":"junior","low":90000,"high":135000},{"level":"mid","low":135000,"high":200000},{"level":"senior","low":200000,"high":340000}],
    "quantum_computing":[{"level":"junior","low":100000,"high":145000},{"level":"mid","low":145000,"high":210000},{"level":"senior","low":210000,"high":330000}],
    "data_engineering": [{"level":"junior","low":85000,"high":125000},{"level":"mid","low":125000,"high":175000},{"level":"senior","low":175000,"high":255000}],
    "cloud_architect":  [{"level":"junior","low":100000,"high":145000},{"level":"mid","low":145000,"high":200000},{"level":"senior","low":200000,"high":320000}],
    "architecture":     [{"level":"staff","low":150000,"high":200000},{"level":"principal","low":200000,"high":260000},{"level":"distinguished","low":260000,"high":400000}],
}


# Dynamic track generation instance
_DYNAMIC_TRACK_GENERATOR = DynamicTrackGenerator()


async def generate_dynamic_tracks(goal: str, market_data: dict) -> dict[str, dict]:
    """Generate dynamic tracks for a career goal."""
    from career_intelligence_pipeline import get_dynamic_tracks
    return get_dynamic_tracks(goal, market_data)


async def detect_track_async(goal: str, market_data: dict) -> str:
    """
    Detect track using dynamic track generation.
    Falls back to keyword matching if dynamic generation fails.
    Returns a TRACKS key if matched, or None to signal fully dynamic generation.
    """
    try:
        goal_lower = goal.lower()
        # Check for exact track keyword matches first
        for track_id, track in TRACKS.items():
            for kw in track["keywords"]:
                if kw in goal_lower:
                    return track_id

        # Generate dynamic tracks to check if market data has useful skills
        dynamic_tracks = await generate_dynamic_tracks(goal, market_data)
        if dynamic_tracks and dynamic_tracks.get(goal, {}).get("core_skills"):
            # Not matched to any hardcoded track — return None for pure dynamic generation
            return None

        logger.warning("No dynamic tracks generated for goal: %s", goal)
        return None

    except Exception as exc:
        logger.error("Error in detect_track_async: %s", exc)
        return None


def detect_track(goal: str) -> str:
    """
    Synchronous keyword-based track detection.
    Prefer detect_track_async() in async contexts for market-data-aware detection.
    """
    return _detect_track_sync(goal)


TIMELINE_WEEKS: dict[SkillLevel, int] = {
    SkillLevel.BEGINNER: 78,
    SkillLevel.INTERMEDIATE: 52,
    SkillLevel.ADVANCED: 26,
}


def adjust_timeline(base_weeks: int, weekly_hours: int) -> int:
    ratio = 10 / max(weekly_hours, 1)
    return max(4, round(base_weeks * ratio))


def topological_sort(skills: list[str], graph: dict[str, dict]) -> list[str]:
    in_degree: dict[str, int] = defaultdict(int)
    adj: dict[str, list[str]] = defaultdict(list)
    skill_set = set(skills)
    for sk in skills:
        for prereq in graph.get(sk, {}).get("prerequisites", []):
            if prereq in skill_set:
                in_degree[sk] += 1
                adj[prereq].append(sk)
    queue = deque(sk for sk in skills if in_degree[sk] == 0)
    result: list[str] = []
    while queue:
        sk = queue.popleft()
        result.append(sk)
        for nb in adj[sk]:
            in_degree[nb] -= 1
            if in_degree[nb] == 0:
                queue.append(nb)
    if len(result) != len(skills):
        missing = [sk for sk in skills if sk not in result]
        logger.warning("Cycle detected in skill graph for %s; appending unsorted: %s", skills, missing)
        result.extend(missing)
    return result


def _build_market_insight(skill_key: str, freq: float, job_pct: float, trend: str) -> str:
    """Build a data-driven, evidence-backed insight sentence from live scrape signals."""
    pct_str = f"{round(job_pct * 100)}%" if job_pct > 0 else "a significant share"
    name = skill_key.replace("_", " ").title()
    trend_phrases = {
        "rising": f"demand for {name} is accelerating",
        "stable": f"{name} maintains steady demand",
        "declining": f"{name} demand is softening but remains relevant",
    }
    base = trend_phrases.get(trend, f"{name} is in demand")
    if job_pct > 0.5:
        return f"{base} - appearing in {pct_str} of scraped job postings this week."
    elif job_pct > 0.2:
        return f"{base}, mentioned in {pct_str} of current openings."
    else:
        return f"{base} - a differentiating skill in competitive job searches."


def _normalize_country_name(country: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", country.lower()).strip()


def _load_ppp_multipliers() -> dict[str, float]:
    """Load PPP multipliers from the bundled JSON file."""
    try:
        ppp_path = Path(__file__).parent / "ppp_data.json"
        with ppp_path.open(encoding="utf-8") as file_obj:
            data = json.load(file_obj)
        multipliers = {
            _normalize_country_name(country): float(multiplier)
            for country, multiplier in data.get("multipliers", {}).items()
        }
        return multipliers or {"default": 0.65}
    except Exception:
        logger.exception("Failed to load PPP multiplier data; using default multiplier")
        return {"default": 0.65}


_PPP_MULTIPLIERS: dict[str, float] = {}


def get_ppp_multiplier(country: str) -> float:
    global _PPP_MULTIPLIERS
    if not _PPP_MULTIPLIERS:
        _PPP_MULTIPLIERS = _load_ppp_multipliers()

    country_key = _normalize_country_name(country)
    if not country_key:
        return _PPP_MULTIPLIERS.get("default", 0.65)

    exact_match = _PPP_MULTIPLIERS.get(country_key)
    if exact_match is not None:
        return exact_match

    country_tokens = set(country_key.split())
    for key, value in _PPP_MULTIPLIERS.items():
        if key == "default" or len(key) < 4:
            continue
        key_tokens = set(key.split())
        if key_tokens and (key_tokens <= country_tokens or country_tokens <= key_tokens):
            return value

    return _PPP_MULTIPLIERS.get("default", 0.65)


def _generate_sparkline(skill_key: str, trend_score: float, freq_score: float, db_snapshots: list[dict] | None = None) -> list[float]:
    """Generate a 7-point sparkline. Uses DB snapshots if available, otherwise synthesises plausible data."""
    import random
    if db_snapshots and len(db_snapshots) >= 7:
        return [round(s.get(skill_key, 0.0), 3) for s in db_snapshots[-7:]]
    # Synthesise: use trend to shape the curve
    base = max(0.05, freq_score)
    points = []
    val = base * random.uniform(0.7, 0.9)
    for i in range(7):
        noise = random.uniform(-0.04, 0.06)
        trend_push = trend_score * 0.03 * i
        val = max(0.01, min(1.0, val + noise + trend_push))
        points.append(round(val, 3))
    return points


def _detect_track_sync(goal: str) -> str:
    """Synchronous keyword-based track detection (no market data). Used as a fallback."""
    goal_lower = goal.lower()
    scores: dict[str, int] = {}
    for track_id, track in TRACKS.items():
        score = sum(len(kw.split()) for kw in track["keywords"] if kw in goal_lower)
        scores[track_id] = score
    best = max(scores, key=lambda k: scores[k])
    return best if scores[best] > 0 else "ml"


class RoadmapEngine:

    def generate(
        self,
        goal: str,
        skill_level: SkillLevel,
        country: str,
        weekly_hours: int,
        market_data: dict,
        analyzer_scores: dict[str, dict],
        job_id: str,
        track_id: Optional[str] = None,
    ) -> RoadmapResult:

        # Compute job-posting frequency per skill from live data
        job_pct: dict[str, float] = self._compute_job_pct(market_data)

        if track_id is None:
            goal_lower = goal.lower()
            detected = None
            for tid, tdata in TRACKS.items():
                if any(kw in goal_lower for kw in tdata["keywords"]):
                    detected = tid
                    break
            track_id = detected

        if track_id is None or track_id not in TRACKS:
            # ── Fully dynamic mode: use DynamicTrackGenerator as primary source ──
            from career_intelligence_pipeline import get_dynamic_tracks
            dynamic = get_dynamic_tracks(goal, market_data)
            dynamic_track = dynamic.get(goal, {})
            core_skills = dynamic_track.get("core_skills", [])

            if not core_skills:
                # Fallback: extract from market data job tags
                for job in market_data.get("job_listings", []):
                    for tag in job.get("tags", []):
                        if isinstance(tag, str):
                            tag_key = tag.lower().replace(" ", "_").replace("-", "_")
                            if tag_key not in core_skills:
                                core_skills.append(tag_key)

            track = {
                "name": dynamic_track.get("name", goal.title()),
                "description": dynamic_track.get("description", f"Career roadmap for {goal}"),
                "core_skills": core_skills,
                "keywords": dynamic_track.get("keywords", [goal.lower()]),
                "salary_range": dynamic_track.get("salary_range", [50000, 120000]),
            }

            # Build skill list: prefer SKILL_GRAPH data, supplement with dynamic skills
            phase_filter = {
                SkillLevel.BEGINNER:     {"foundation", "core"},
                SkillLevel.INTERMEDIATE: {"core", "specialization"},
                SkillLevel.ADVANCED:     {"specialization", "advanced"},
            }
            allowed = phase_filter[skill_level]

            graph_skills = [sk for sk in core_skills if sk in SKILL_GRAPH and SKILL_GRAPH[sk]["phase"] in allowed]
            dynamic_skills = [sk for sk in core_skills if sk not in SKILL_GRAPH]
            selected = list(dict.fromkeys(graph_skills + dynamic_skills))
        else:
            # ── Hardcoded track mode ──
            track = TRACKS[track_id]
            phase_filter = {
                SkillLevel.BEGINNER:     {"foundation", "core"},
                SkillLevel.INTERMEDIATE: {"core", "specialization"},
                SkillLevel.ADVANCED:     {"specialization", "advanced"},
            }
            allowed = phase_filter[skill_level]
            track_skills = [sk for sk, d in SKILL_GRAPH.items() if track_id in d.get("tracks", [])]
            selected = list(dict.fromkeys(
                [sk for sk in track_skills if SKILL_GRAPH[sk]["phase"] in allowed] +
                [sk for sk in track["core_skills"] if sk in SKILL_GRAPH and SKILL_GRAPH[sk]["phase"] in allowed]
            ))

        sorted_skills = topological_sort(selected, SKILL_GRAPH)

        # Timeline
        total_hours = 0
        for sk in sorted_skills:
            if sk in SKILL_GRAPH:
                hrs = SKILL_GRAPH[sk]["hours"]
            else:
                hrs = 30  # Default hours for dynamic skills not in SKILL_GRAPH
            total_hours += hrs
        computed_weeks = max(4, round(total_hours / max(weekly_hours, 1)))

        # PPP salary adjustment
        ppp = get_ppp_multiplier(country)
        local_currency = "USD" if ppp >= 0.9 else self._local_currency_label(country)

        # Build SkillNodes with live data insights + sparklines
        skill_nodes: list[SkillNode] = []
        for sk in sorted_skills:
            s = analyzer_scores.get(sk, {})
            freq = s.get("frequency_score", 0.1)
            trend_s = s.get("trend_score", 0.1)
            rel = s.get("relevance_score", 0.2)
            total_s = freq * 0.4 + trend_s * 0.3 + rel * 0.3
            jp = job_pct.get(sk, 0.0)
            trend_label = "rising" if trend_s > 0.35 else ("stable" if total_s > 0.18 else "declining")
            already = False
            sparkline = _generate_sparkline(sk, trend_s, freq)
            if sk in SKILL_GRAPH:
                d = SKILL_GRAPH[sk]
                phase = d["phase"]
                prereqs = [p.replace("_", " ").title() for p in d.get("prerequisites", []) if p in selected]
                resources = d.get("resources", [])
                why = d.get("why", "")
                s_impact = d.get("salary_impact", "")
                hours = d["hours"] // 2 if already else d["hours"]
            else:
                # Dynamic skill not in hardcoded graph — estimate values
                idx = sorted_skills.index(sk)
                if idx < len(sorted_skills) * 0.3:
                    phase = "foundation"
                elif idx < len(sorted_skills) * 0.7:
                    phase = "core"
                elif idx < len(sorted_skills) * 0.9:
                    phase = "specialization"
                else:
                    phase = "advanced"
                prereqs = []
                resources = [{"title": "Search online courses & tutorials", "url": f"https://www.google.com/search?q=learn+{sk.replace('_', '+')}"}]
                why = f"{sk.replace('_', ' ').title()} skills are in demand based on live market data."
                s_impact = ""
                hours = 30 // 2 if already else 30
            skill_nodes.append(SkillNode(
                name=sk.replace("_", " ").title(),
                key=sk,
                hours=hours,
                phase=phase,
                prerequisites=prereqs,
                resources=resources,
                why=why,
                frequency_score=round(freq, 3),
                trend_score=round(trend_s, 3),
                relevance_score=round(rel, 3),
                market_insight=_build_market_insight(sk, freq, jp, trend_label),
                salary_impact=s_impact,
                sparkline=sparkline,
            ))

        phases = self._build_phases(skill_nodes, computed_weeks, skill_level, market_data, job_pct, weekly_hours)
        dag_edges = self._build_dag_edges(sorted_skills)
        market_demand = self._build_market_demand(track["core_skills"], analyzer_scores, job_pct)
        universities = self._match_universities(track_id, country, market_data.get("universities", []))
        scholarships = self._match_scholarships(goal, country, market_data.get("scholarships", []))
        salary_bands = self._build_salary_bands(track_id, ppp, local_currency)
        summary = self._generate_summary(
            goal, track, skill_level, computed_weeks, weekly_hours,
            sorted(skill_nodes, key=lambda n: n.total_score, reverse=True)[:5],
            country, None, job_pct,
        )

        # Data signals for template
        total_jobs_scraped = len(market_data.get("job_listings", []))
        top_scraped_skill = max(job_pct, key=job_pct.get) if job_pct else ""

        internships = self._match_internships(track_id, country, market_data.get("internships", []))
        top_companies = self._extract_top_companies(market_data)
        trend_analysis = self._build_trend_analysis(market_data, goal, job_pct)

        return RoadmapResult(
            job_id=job_id,
            goal=goal,
            skill_level=skill_level,
            country=country,
            weekly_hours=weekly_hours,
            total_weeks=computed_weeks,
            executive_summary=summary,
            phases=phases,
            market_demand=market_demand,
            universities=universities,
            scholarships=scholarships,
            top_skills=sorted(skill_nodes, key=lambda n: n.total_score, reverse=True)[:15],
            salary_bands=salary_bands,
            internships=internships,
            top_companies=top_companies,
            trend_analysis=trend_analysis,
            gap_analysis=None,
            dag_edges=dag_edges,
            data_signals={
                "total_jobs_scraped": total_jobs_scraped,
                "top_scraped_skill": top_scraped_skill,
                "github_repos_analysed": len(market_data.get("github_trends", [])),
                "arxiv_papers_analysed": len(market_data.get("arxiv_papers", [])),
                "ppp_multiplier": ppp,
                "local_currency": local_currency,
            },
            data_sources=["GitHub Trending", "HN Hiring", "arXiv CS.AI", "RemoteOK", "WeWorkRemotely"],
            created_at=datetime.now(UTC),
        )

    # ── helpers ────────────────────────────────────────────────────────────────

    def _normalise_skill_name(self, s: str) -> str:
        return s.lower().strip().replace("-", " ").replace("_", " ")

    def _local_currency_label(self, country: str) -> str:
        currency_map = {
            "argentina": "ARS",
            "australia": "AUD",
            "bangladesh": "BDT",
            "brazil": "BRL",
            "canada": "CAD",
            "china": "CNY",
            "colombia": "COP",
            "denmark": "DKK",
            "egypt": "EGP",
            "india": "INR",
            "indonesia": "IDR",
            "japan": "JPY",
            "kenya": "KES",
            "malaysia": "MYR",
            "mexico": "MXN",
            "new zealand": "NZD",
            "nigeria": "NGN",
            "norway": "NOK",
            "pakistan": "PKR",
            "peru": "PEN",
            "philippines": "PHP",
            "poland": "PLN",
            "russia": "RUB",
            "singapore": "SGD",
            "south africa": "ZAR",
            "south korea": "KRW",
            "sweden": "SEK",
            "switzerland": "CHF",
            "thailand": "THB",
            "turkey": "TRY",
            "uk": "GBP",
            "ukraine": "UAH",
            "united kingdom": "GBP",
            "vietnam": "VND",
        }
        country_key = _normalize_country_name(country)
        exact_match = currency_map.get(country_key)
        if exact_match:
            return exact_match
        country_tokens = set(country_key.split())
        for key, value in currency_map.items():
            key_tokens = set(key.split())
            if key_tokens and (key_tokens <= country_tokens or country_tokens <= key_tokens):
                return value
        return "USD"

    def _build_salary_bands(self, track_id: str | None, ppp: float, local_currency: str) -> list[SalaryBand]:
        bands = []
        salary_source = SALARY_BANDS.get(track_id, []) if track_id else []
        if salary_source:
            for b in salary_source:
                local_low = round(b["low"] * ppp / 1000) * 1000 if ppp < 0.95 else None
                local_high = round(b["high"] * ppp / 1000) * 1000 if ppp < 0.95 else None
                bands.append(SalaryBand(
                    level=b["level"], low=b["low"], high=b["high"],
                    local_low=local_low, local_high=local_high,
                    local_currency=local_currency if ppp < 0.95 else "USD",
                    ppp_multiplier=ppp,
                ))
        else:
            # Dynamic/salary estimate for unknown tracks
            for level, mult in [("Entry", 1.0), ("Mid", 1.5), ("Senior", 2.5)]:
                low = int(40000 * mult)
                high = int(80000 * mult)
                local_low = round(low * ppp / 1000) * 1000 if ppp < 0.95 else None
                local_high = round(high * ppp / 1000) * 1000 if ppp < 0.95 else None
                bands.append(SalaryBand(
                    level=level, low=low, high=high,
                    local_low=local_low, local_high=local_high,
                    local_currency=local_currency if ppp < 0.95 else "USD",
                    ppp_multiplier=ppp,
                ))
        return bands

    def _compute_job_pct(self, market_data: dict) -> dict[str, float]:
        """What % of scraped jobs mention each skill key."""
        import re
        jobs = market_data.get("job_listings", []) + market_data.get("hn_jobs", [])
        if not jobs:
            return {}
        # Collect skill keys from both hardcoded graph and live job tags
        all_skill_keys: set[str] = set(SKILL_GRAPH.keys())
        for job in jobs:
            for tag in job.get("tags", []):
                if isinstance(tag, str):
                    all_skill_keys.add(tag.lower().replace(" ", "_").replace("-", "_"))
        patterns: dict[str, re.Pattern] = {}
        for sk in all_skill_keys:
            alias = sk.replace("_", " ")
            patterns[sk] = re.compile(r"\b" + re.escape(alias) + r"\b", re.IGNORECASE)

        counts: dict[str, int] = defaultdict(int)
        for job in jobs:
            text = (job.get("text", "") + " " + " ".join(t for t in (job.get("tags") or []) if isinstance(t, str))).lower()
            for sk in all_skill_keys:
                if patterns[sk].search(text) or sk in text.split():
                    counts[sk] += 1
        total = len(jobs)
        return {sk: round(c / total, 3) for sk, c in counts.items()}

    def analyze_jd(
        self,
        jd_text: str,
        weekly_hours: int = 10,
    ) -> dict:
        """Extract required skills from a job description and estimate study time."""
        from career_intelligence_pipeline import extract_skills_regex, extract_skills_keybert, normalize_skill
        # Combine extraction methods
        regex_skills = extract_skills_regex(jd_text)
        kw_skills = [normalize_skill(kw) for kw, _ in extract_skills_keybert(jd_text, top_n=25)]
        all_jd_skills = list(dict.fromkeys(regex_skills + kw_skills))

        required_skills: list[str] = []
        weeks_map: dict[str, int] = {}
        for sk in all_jd_skills:
            sk_norm = self._normalise_skill_name(sk)
            graph_key = sk_norm.replace(" ", "_")
            hrs = SKILL_GRAPH.get(graph_key, {}).get("hours", 20)
            weeks_map[sk] = max(1, round(hrs / max(weekly_hours, 1)))
            required_skills.append(sk)

        return {
            "jd_skills": required_skills,
            "required_skills": required_skills,
            "missing": required_skills,
            "matched": [],
            "weeks_per_skill": weeks_map,
            "readiness_pct": 0,
            "total_weeks_to_close": sum(weeks_map.values()),
        }

    def compare_goals(
        self,
        goal_a: str,
        goal_b: str,
        skill_level: SkillLevel = SkillLevel.BEGINNER,
        weekly_hours: int = 10,
    ) -> "ComparisonResult":
        """Compare two career goals and return a 3-column skill diff."""
        from models import ComparisonResult
        track_a = _detect_track_sync(goal_a)
        track_b = _detect_track_sync(goal_b)

        def _get_skills(track_id: str) -> set[str]:
            return {
                sk for sk, d in SKILL_GRAPH.items()
                if track_id in d.get("tracks", [])
            }

        skills_a = _get_skills(track_a)
        skills_b = _get_skills(track_b)
        only_a = sorted(skills_a - skills_b)
        shared = sorted(skills_a & skills_b)
        only_b = sorted(skills_b - skills_a)

        def _weeks(skills: set[str]) -> int:
            hours = sum(SKILL_GRAPH.get(sk, {}).get("hours", 20) for sk in skills)
            return max(4, round(hours / max(weekly_hours, 1)))

        return ComparisonResult(
            goal_a=goal_a, goal_b=goal_b,
            track_a=TRACKS.get(track_a, {}).get("name", track_a),
            track_b=TRACKS.get(track_b, {}).get("name", track_b),
            skills_only_a=[s.replace("_", " ").title() for s in only_a],
            skills_shared=[s.replace("_", " ").title() for s in shared],
            skills_only_b=[s.replace("_", " ").title() for s in only_b],
            weeks_a=_weeks(skills_a),
            weeks_b=_weeks(skills_b),
            switch_cost_weeks=_weeks(skills_b - skills_a),
        )

    def _build_dag_edges(self, skills: list[str]) -> list[DAGEdge]:
        skill_set = set(skills)
        edges = []
        for sk in skills:
            for prereq in SKILL_GRAPH.get(sk, {}).get("prerequisites", []):
                if prereq in skill_set:
                    edges.append(DAGEdge(source=prereq, target=sk))
        return edges

    def _build_phases(
        self,
        skills: list[SkillNode],
        total_weeks: int,
        level: SkillLevel,
        market_data: dict,
        job_pct: dict[str, float],
        weekly_hours: int = 10,
    ) -> list[Phase]:
        phase_order = ["foundation", "core", "specialization", "advanced"]
        phase_meta = {
            "foundation": ("Foundations", "Build the essential knowledge that underpins all advanced learning."),
            "core":       ("Core Skills", "Develop the primary competencies that define your career track."),
            "specialization": ("Specialisation", "Deepen expertise in the highest-demand areas."),
            "advanced":   ("Advanced Mastery", "Master cutting-edge techniques and production-grade practices."),
        }
        grouped: dict[str, list[SkillNode]] = defaultdict(list)
        for sk in skills:
            grouped[sk.phase].append(sk)

        hours_per_week = max(1, weekly_hours)
        result = []
        for i, pk in enumerate([p for p in phase_order if grouped.get(p)]):
            phase_skills = grouped[pk]
            phase_hours = sum(s.hours for s in phase_skills)
            phase_weeks = max(1, round(phase_hours / hours_per_week))
            name, desc = phase_meta.get(pk, (pk.title(), ""))
            top_sk = max(phase_skills, key=lambda s: job_pct.get(s.key, 0), default=None)
            if top_sk and job_pct.get(top_sk.key, 0) > 0:
                pct = round(job_pct[top_sk.key] * 100)
                insight = f"{top_sk.name} is the most-demanded skill in this phase, appearing in {pct}% of relevant job postings."
            else:
                insight = f"This phase covers {len(phase_skills)} skills totalling ~{phase_hours} hours of study."
            result.append(Phase(
                number=i + 1, name=name, duration_weeks=phase_weeks,
                skills=phase_skills, description=desc, phase_insight=insight,
            ))
        return result

    def _build_market_demand(
        self,
        core_skills: list[str],
        scores: dict[str, dict],
        job_pct: dict[str, float],
    ) -> list[MarketDemand]:
        top_employers = {
            "python": ["Google","Meta","OpenAI","Stripe","Shopify"],
            "pytorch": ["Meta AI","OpenAI","Hugging Face","DeepMind","NVIDIA"],
            "kubernetes": ["Google","AWS","Microsoft","Red Hat","Datadog"],
            "aws": ["Amazon","Netflix","Airbnb","Lyft","Twilio"],
            "react": ["Meta","Airbnb","Twitter/X","Atlassian","Vercel"],
            "tensorflow": ["Google Brain","Intel","Qualcomm","ARM","IBM"],
            "golang": ["Google","Docker","Cloudflare","Uber","Dropbox"],
            "sql": ["Snowflake","Databricks","Confluent","dbt Labs","Mode"],
            "docker": ["Docker","AWS","Microsoft","Red Hat","Pivotal"],
            "deep_learning": ["DeepMind","OpenAI","Google Brain","Meta AI","Tesla"],
            "rust": ["Mozilla","Cloudflare","Microsoft","Figma","Discord"],
            "kafka": ["Confluent","LinkedIn","Uber","Airbnb","Twitter/X"],
            "langchain": ["LangChain","Anthropic","OpenAI","Scale AI","Cohere"],
        }
        result = []
        for sk in core_skills[:12]:
            s = scores.get(sk, {})
            freq = s.get("frequency_score", 0.2)
            trend = s.get("trend_score", 0.15)
            total = freq * 0.6 + trend * 0.4
            jp = job_pct.get(sk, 0.0)
            trend_label = "rising" if trend > 0.3 else ("stable" if trend > 0.12 else "declining")
            sparkline = _generate_sparkline(sk, trend, freq)
            result.append(MarketDemand(
                skill=sk.replace("_", " ").title(),
                demand_score=round(total, 2),
                trend=trend_label,
                job_count_estimate=max(500, round(total * 50000)),
                top_employers=top_employers.get(sk, ["Various Fortune 500 companies"]),
                job_posting_pct=round(jp * 100, 1),
                sparkline=sparkline,
            ))
        return sorted(result, key=lambda d: d.demand_score, reverse=True)

    def _match_universities(self, track_id, country, universities):
        keywords = {
            "ml":            ["machine learning","computer science","cs","ai","computing"],
            "ai_research":   ["computer science","cs","ai","ml","informatics"],
            "data_science":  ["data science","computer science","statistics","computing"],
            "backend":       ["computer science","cs","software","computing"],
            "frontend":      ["computer science","cs","web","computing"],
            "cybersecurity": ["cybersecurity","computer science","information security"],
            "devops":        ["computer science","cs","software","cloud"],
            "bioinformatics":["bioinformatics","computational biology","genomics"],
        }.get(track_id, ["computer science", "engineering", "technology"])
        result = []
        for u in universities:
            prog = u.get("program","").lower()
            uc = u.get("country","").lower()
            score = sum(2 for kw in keywords if kw in prog)
            score += 3 if country.lower() in uc else (1 if uc in ("global","usa","uk") else 0)
            if score >= 1:
                result.append(University(
                    name=u.get("name", ""), country=u.get("country", ""),
                    ranking=u.get("ranking", 999), program=u.get("program", ""),
                    url=u.get("url", ""),
                    description=f"Ranked #{u.get('ranking', 999)} globally for {u.get('program', 'research')}.",
                ))
        return sorted(result, key=lambda u: u.ranking)[:8]

    def _match_scholarships(self, goal, country, scholarships):
        result = []
        cl = country.lower()
        for s in scholarships:
            sc = s.get("country","").lower()
            elig = s.get("eligibility","").lower()
            score = 0.0
            if cl in sc or sc in cl:
                score += 0.4
            if any(w in sc for w in ["global", "international"]):
                score += 0.2
            if any(w in elig for w in ["stem", "technology", "science", "engineering", "computing"]):
                score += 0.3
            if score > 0:
                result.append(Scholarship(
                    name=s.get("name", ""), country=s.get("country", ""),
                    amount=s.get("amount", ""), deadline=s.get("deadline", ""),
                    eligibility=s.get("eligibility", ""), url=s.get("url", ""),
                    relevance_score=score,
                ))
        return sorted(result, key=lambda sc: sc.relevance_score, reverse=True)[:6]

    def _match_internships(self, track_id, country, internships):
        keywords = {
            "ml":            ["machine learning","computer vision","deep learning"],
            "ai_research":   ["ai research","neural networks","natural language"],
            "data_science":  ["data science","statistics","analytics"],
            "backend":       ["backend","api development","microservices"],
            "frontend":      ["frontend","ui","web development"],
            "cybersecurity": ["cybersecurity","security engineering","threat analysis"],
            "devops":        ["devops","cloud infrastructure","ci/cd"],
            "bioinformatics":["bioinformatics","genomics","computational biology"],
        }.get(track_id, []) if track_id else []
        scored = []
        for i in internships:
            title = i.get("title", "").lower()
            company = i.get("company", "").lower()
            location = i.get("location", "").lower()
            score = 0.0
            if any(kw in title for kw in keywords):
                score += 0.5
            if country.lower() in location or location in country.lower():
                score += 0.3
            if any(kw in company for kw in ["tech", "ai", "ml", "data"]):
                score += 0.2
            if score >= 0.5:
                scored.append((score, Internship(
                    title=i.get("title", ""), company=i.get("company", ""),
                    location=i.get("location", ""), duration=i.get("duration", ""),
                    stipend=i.get("stipend", ""), source=i.get("source", ""),
                    url=i.get("url", ""),
                )))
        return [item for _, item in sorted(scored, key=lambda x: x[0], reverse=True)][:6]

    def _extract_top_companies(self, market_data: dict) -> list[TopCompany]:
        companies = {}
        job_listings = market_data.get("job_listings", [])
        for job in job_listings:
            company = job.get("company", "")
            if company:
                companies[company] = companies.get(company, 0) + 1
        top_companies = []
        for company, count in sorted(companies.items(), key=lambda x: x[1], reverse=True)[:15]:
            top_companies.append(TopCompany(
                name=company,
                industry="Tech",
                internship_count=max(1, min(10, count // 3)),
                location="Global",
                popularity_score=min(100, count * 5),
                trend_score=50,
            ))
        return top_companies

    def _build_trend_analysis(self, market_data: dict, goal: str, job_pct: dict[str, float] | None = None) -> list[TrendAnalysis]:
        skills = []
        # Use job_pct skills first (these come from live data)
        if job_pct:
            skills = list(job_pct.keys())
        if not skills:
            for skill in SKILL_GRAPH:
                if goal.lower() in skill.lower():
                    skills.append(skill)
        if not skills:
            skills = list(SKILL_GRAPH.keys())[:15]

        trends = []
        for skill in skills[:15]:
            freq_score = job_pct.get(skill, 0.2) if job_pct else 0.2
            trend_score = freq_score * 0.8 + 0.15
            future_proofing = freq_score > 0.25 and trend_score > 0.2
            trends.append(TrendAnalysis(
                skill=skill,
                trend_score=round(trend_score, 3),
                demand_velocity=min(100, round(trend_score * 100)),
                future_proofing_score=future_proofing,
                emerging_opportunity=future_proofing,
            ))
        return sorted(trends, key=lambda t: t.trend_score, reverse=True)

    def _generate_summary(self, goal, track, level, weeks, weekly_hours, top_skills, country, gap, job_pct):
        months = round(weeks / 4.3, 1)
        top_names = ", ".join(s.name for s in top_skills[:3])
        top_jp_sk = max(job_pct, key=job_pct.get) if job_pct else None
        data_point = ""
        if top_jp_sk and job_pct[top_jp_sk] > 0:
            data_point = (
                f" Live job-board data shows {top_jp_sk.replace('_',' ').title()} "
                f"appearing in {round(job_pct[top_jp_sk]*100)}% of relevant postings this week."
            )
        gap_note = ""
        if gap and getattr(gap, "gap_score", 0) > 0:
            gap_note = (
                f" Your gap analysis shows you already have {len(gap.known_skills)} of {len(gap.known_skills)+len(gap.missing_skills)} "
                f"required skills ({gap.readiness_label}), saving an estimated {gap.estimated_weeks_saved} weeks."
            )
        return (
            f"Your personalised {track['name']} roadmap was built from live market intelligence - "
            f"not static templates.{data_point} "
            f"At {level.value} level with {weekly_hours}h/week, you are estimated to be job-ready in "
            f"~{months} months ({weeks} weeks). "
            f"The highest-priority skills right now are: {top_names}.{gap_note} "
            f"{track['description']}"
        )




# ── Merged from: scheduler.py ──────────────────────────────────────
_analyzer = None
_progress: dict[str, dict] = {}
_MAX_PROGRESS_ENTRIES = 500


def _cleanup_old_progress():
    if len(_progress) > _MAX_PROGRESS_ENTRIES:
        oldest = sorted(_progress.items(), key=lambda x: x[1].get("updated_at", 0))[:len(_progress) - _MAX_PROGRESS_ENTRIES]
        for k, _ in oldest:
            _progress.pop(k, None)


def _get_analyzer() -> Analyzer:
    global _analyzer
    if _analyzer is None:
        # Ensure models are loaded before creating Analyzer
        _get_kw_model()
        _get_embed_model()
        _analyzer = Analyzer()
    return _analyzer


def new_job_id() -> str:
    return uuid.uuid4().hex


def get_progress(job_id: str) -> dict:
    return _progress.get(job_id, {"status": "unknown", "progress": 0, "message": "Not found"})


async def _emit(job_id: str, status: str, progress: int, message: str) -> None:
    _progress[job_id] = {"status": status, "progress": progress, "message": message, "updated_at": time.time()}
    _cleanup_old_progress()
    await models.update_job(job_id, status, progress, message)


async def run_job(job_id: str, req: RoadmapRequest) -> None:
    start = time.time()
    try:
        await _emit(job_id, "running", 5, "Scanning industry demand…")

        cache_key = models.make_key(req.goal, req.skill_level, req.country,
                                   str(req.weekly_hours), "")
        cached = await models.get(cache_key)
        if cached:
            await _emit(job_id, "running", 95, "Loading from cache…")
            try:
                result = RoadmapResult.model_validate(cached)
            except Exception:
                result = RoadmapResult(**cached)
            result.cache_hit = True
            await models.finish_job(job_id, result.model_dump_json())
            try:
                await models.record_metric(req.goal, req.skill_level, req.country,
                                             round((time.time()-start)*1000), True)
            except Exception:
                logger.exception("Job %s: metric recording failed (non-fatal)", job_id)
            _progress[job_id] = {"status":"complete","progress":100,"message":"Done"}
            return

        await _emit(job_id, "running", 15, "Fetching live market data from 5 sources…")
        try:
            market_data = await asyncio.wait_for(
                fetch_all_market_data(req.goal), timeout=30
            )
        except asyncio.TimeoutError:
            logger.warning("Job %s: market data fetch timed out", job_id)
            market_data = {"job_listings": [], "hn_jobs": [], "github_trends": [],
                           "arxiv_papers": [], "scholarships": [], "universities": [],
                           "salary_data": [], "internships": []}

        await _emit(job_id, "running", 38, "Extracting skills with KeyBERT…")
        loop = asyncio.get_running_loop()
        try:
            scores = await asyncio.wait_for(
                loop.run_in_executor(None, functools.partial(_get_analyzer().analyze, market_data, req.goal, req.goal)),
                timeout=45,
            )
        except asyncio.TimeoutError:
            logger.warning("Job %s: analyzer timed out after 45s — falling back to regex-only extraction", job_id)
            await _emit(job_id, "running", 45, "NLP timed out — using fast skill extraction…")
            # Graceful fallback: build scores from regex extraction only, skip heavy NLP
            skill_freq: dict[str, int] = {}
            source_weights = [
                ("github_trends", 2),
                ("job_listings", 3),
                ("hn_jobs", 2),
                ("arxiv_papers", 1),
            ]
            for source_key, weight in source_weights:
                for item in market_data.get(source_key, []):
                    text = " ".join(str(v) for v in item.values() if isinstance(v, str))
                    for sk in extract_skills_regex(text):
                        skill_freq[sk] = skill_freq.get(sk, 0) + weight
            if not skill_freq:
                skill_freq = {
                    "python": 30, "machine learning": 25, "deep learning": 20,
                    "tensorflow": 15, "pytorch": 18, "sql": 12, "docker": 10,
                    "kubernetes": 8, "git": 20, "statistics": 12,
                }
            max_freq = max(skill_freq.values(), default=1)
            scores = {
                sk: {
                    "frequency_score": round(v / max_freq, 3),
                    "trend_score": 0.0,
                    "relevance_score": 0.1,
                }
                for sk, v in skill_freq.items()
            }

        await _emit(job_id, "running", 55, "Detecting career track from live signals…")
        track_id = await detect_track_async(req.goal, market_data)
        logger.info("Job %s: detected track '%s' for goal '%s'", job_id, track_id, req.goal)

        await _emit(job_id, "running", 58, "Building dependency graph…")
        _job_engine = RoadmapEngine()
        result = await loop.run_in_executor(
            None,
            functools.partial(
                _job_engine.generate,
                goal=req.goal,
                skill_level=req.skill_level,
                country=req.country,
                weekly_hours=req.weekly_hours,
                market_data=market_data,
                analyzer_scores=scores,
                job_id=job_id,
                track_id=track_id,
            ),
        )

        await _emit(job_id, "running", 82, "Matching universities & scholarships…")
        await _emit(job_id, "running", 92, "Finalising roadmap…")

        result_json = result.model_dump_json()
        await models.set(cache_key, json.loads(result_json))
        await models.finish_job(job_id, result_json)
        try:
            await models.record_metric(req.goal, req.skill_level, req.country,
                                         round((time.time()-start)*1000), False)
        except Exception:
            logger.exception("Job %s: metric recording failed (non-fatal)", job_id)
        _progress[job_id] = {"status":"complete","progress":100,"message":"Done"}

    except asyncio.CancelledError:
        logger.info("Job %s cancelled", job_id)
        current_progress = _progress.get(job_id, {}).get("progress", 0)
        await models.fail_job(job_id, "Job was cancelled", current_progress)
        _progress[job_id] = {"status": "failed", "progress": current_progress, "message": "Cancelled"}
        raise
    except Exception as exc:
        logger.exception("Job %s failed", job_id)
        error_msg = str(exc) if str(exc).strip() else f"{type(exc).__name__} during roadmap generation"
        current_progress = _progress.get(job_id, {}).get("progress", 0)
        await models.fail_job(job_id, error_msg, current_progress)
        _progress[job_id] = {"status": "failed", "progress": current_progress, "message": error_msg}

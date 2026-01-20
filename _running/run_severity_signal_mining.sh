export OPENAI_API_KEY=sk-proj-

python ./libs/models/severity_signals/mine_signals.py   

python ./libs/models/severity_signals/normalize_signals.py

python ./libs/models/severity_signals/merge_signal_concepts.py

python ./libs/models/severity_signals/merge_canonical_concepts_rule_based.py
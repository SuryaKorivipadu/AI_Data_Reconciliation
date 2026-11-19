# AI_Data_Reconciliation
Teradata vs Databricks data reconciliation/comparison using python and AI for data migration projects

Ollama model and request timeout are configurable in `CONFIG` in `compare_data.py`.
The Ollama request timeout defaults to 600 seconds to allow slower local models to
finish; increase it for slower hardware or select a smaller model if requests still
time out.


# Pre-requisites:
- ollama version == 0.40.16
- ollama pull llama3.1
- data in data folder
- python

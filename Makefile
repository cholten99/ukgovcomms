.PHONY: docs docs-open docs-clean

# Where to write the generated overview
DOC_OUT := docs/repo_overview.md
PY      ?= python3
ROOT    ?= .

docs:
	@mkdir -p $(dir $(DOC_OUT))
	@$(PY) tools/generate_repo_docs.py --root $(ROOT) --out $(DOC_OUT) --log-level INFO
	@echo "Wrote $(DOC_OUT)"

# Convenience: open it after generating (works on Linux/macOS)
docs-open: docs
	@{ command -v xdg-open >/dev/null && xdg-open $(DOC_OUT); } \
	 || { command -v open >/dev/null && open $(DOC_OUT); } \
	 || echo "Open $(DOC_OUT) manually."

# Remove the generated doc if you want a clean slate
docs-clean:
	@rm -f $(DOC_OUT)
	@echo "Removed $(DOC_OUT)"

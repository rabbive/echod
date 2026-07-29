# Paper source (IEEE conference format)

Single-file LaTeX source for the project write-up.

## Layout

```
paper/
├── main.tex              # the paper
├── figures/
│   ├── availability.png       # copied from ../results/availability.png
│   ├── energy.png             # copied from ../results/energy.png
│   ├── latency_messages.png   # copied from ../results/latency_messages.png
│   └── dashboard.png          # TODO: add a screenshot of the Flask dashboard
│                              # (uncomment the \begin{figure} block in main.tex
│                              # once you've dropped the PNG here)
└── README.md
```

## Build

No external dependencies beyond a standard TeX distribution with IEEEtran:

```bash
cd paper
pdflatex main.tex
pdflatex main.tex     # second pass resolves \ref cross-references
```

(No `.bib` file is used; references are inline `\bibitem` entries. If you
switch to BibTeX, run `bibtex main && pdflatex main.tex && pdflatex main.tex`.)

## What's generated where

| Content                | Source                                     |
|------------------------|--------------------------------------------|
| Topology figure        | TikZ inline in `main.tex` (Fig. 1)         |
| State-machine figure   | TikZ inline in `main.tex` (Fig. 2)         |
| Partition timeline     | TikZ inline in `main.tex` (Fig. 3)         |
| MQTT topology          | TikZ inline in `main.tex` (Fig. 4)         |
| Dashboard screenshot   | **You supply** `figures/dashboard.png`     |
| Latency/messages chart | `figures/latency_messages.png` from results/ |
| Energy chart           | `figures/energy.png` from results/         |
| Availability chart     | `figures/availability.png` from results/   |
| Reconcile pseudocode   | `algorithmic` inline in `main.tex`         |

## Regenerating the empirical charts

```bash
cd ..   # back to repo root
python3 -m simulation.main \
    --coordinators 5 --leaves 10 \
    --duration 5 --battery-drain 0.01 \
    --charts --output-dir results
cp results/{availability,energy,latency_messages}.png paper/figures/
```

## Things flagged in the draft that you may want to touch

- Acknowledgment line (currently generic; add funding/advisor credit if
  required by your department).
- Dashboard screenshot (`figures/dashboard.png`) — the figure environment
  is currently commented out; uncomment it once the PNG exists (grab one
  from `bash scripts/demo.sh`).
- **Done:** the harness-asymmetry caveat is resolved — all three
  protocols now replay an identical seeded workload
  (`simulation/workload.py`), Table II and the three chart figures were
  regenerated from the matched-workload harness, and a new
  Section~IV (`echoD: A Raft/ECHO Hybrid`) documents the second
  protocol, its six optimizations, and its relationship to prior art.
- The paper is currently ~10 pages after adding the echoD section; check
  your venue's page limit and trim the "Related Work" or "Discussion"
  subsections if you need to shorten it.
- Remaining planned experiments (seed sweep, physical hardware
  validation with real battery telemetry) are listed in §V-F
  ("Planned extensions") — the Phase~2 MQTT harness
  (`scripts/run_experiment.sh`) already supports all three protocols and
  is the basis for the hardware run once equipment arrives.

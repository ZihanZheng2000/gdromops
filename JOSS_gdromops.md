# gdromops: A Python package for simulating reservoir operations using GDROM rules

## Summary

Reservoir operations play a crucial role in water management and hydrological modeling, regulating inflows and releases to meet diverse human and environmental demands (Poff et al., 1997). However, most current large-scale hydrologic models still rely on simplified or unrealistic reservoir representations, leading to substantial uncertainties and biases in model parameterization and streamflow simulation (Vora et al., 2024). To address this limitation, we developed a Python package gdromops, which provides accurate, data-driven reservoir operation rules for 2,017 reservoirs across the Contiguous United States (CONUS).

The gdromops package offers a reproducible, open-source, and lightweight interface for applying 2,017 pre-trained GDROM (Generic Data-Driven Reservoir Operation Models) rules in reservoir simulations (Zheng et al., 2025). It automates rule loading, release-simulation and storage-update computation. By integrating gdromops, users can replace simplified level-pool or rule-curve methods with realistic, data-driven reservoir operation behavior. The package can be used as a standalone simulation engine or embedded within large-scale routing frameworks—such as NOAA’s National Water Model and T-Route—to improve both research and operational forecasting applications.

---

## Statement of Need

Reservoirs perform as both a major human intervention to streamflow and a major water management infrastructure for various water use purposes. Developing a realistic reservoir representation for large-scale water models is paramount for hydrologic simulation accuracy and further for enhanced water resources management. However, the lack of details regarding real-world reservoir operations has impeded the development of a generic reservoir model compatible with rainfall-runoff processes. As a result, current large-scale water models go with unrealistic reservoir operation models and end with uncertainties and errors in model parametrization and simulation (Vora et al. 2024). For example, the current U.S. National Water Model (NWM) at National Water Center (NWC) still uses simple methods, such as the level pool method or prescribed operation curves for reservoir routing, highlighting an outstanding need for improvement (Cosgrove et al., 2024). These simplifications limit model performance in simulating downstream flows, reservoir storage dynamics, and water allocation decisions.

GDROMs were developed under the CIROH project to create a generic, data-driven representation of reservoir operations. The initial development produced models for 452 large CONUS reservoirs using at least 15 years of daily operation records (Chen et al., 2022); model inputs and outputs are publicly available on HydroShare (Li et al., 2023). The GDROM collection has since been expanded to cover over 2,000 reservoirs (area > 1 km²), including some reservoirs with limited observational data (Zheng et al., 2025), and the updated datasets and rule sets are also shared on HydroShare (Zheng et al., 2025). The GDROM framework follows four guiding principles: interpretability (a modular, transparent structure that reveals operation behaviors); generality (four universal inputs—inflow, storage, day of year, and PDSI—to represent diverse operation patterns); compatibility (a simple ID crosswalk and efficient coupling with large-scale hydrologic models); and reliability (validation using 15–30 years of observations across hundreds of reservoirs, demonstrating improved streamflow simulation skill compared with traditional level-pool methods).

To make these accurate and data-driven reservoir operation models accessible and usable within large-scale hydrologic modeling frameworks, we developed the gdromops python package. This package translates the GDROM framework into a practical, lightweight, and reproducible Python tool that can be directly integrated into existing modeling workflows. Instead of relying on external data retrieval or manual setup, gdromops encapsulates all pre-trained operation rules, metadata, and drought index inputs, providing a plug-and-play solution for realistic reservoir representation. In doing so, it bridges the gap between research and operational modeling—turning scientifically validated GDROM algorithms into an easy-to-use, open-source component. Through gdromops, realistic reservoir operations can now be systematically incorporated into models such as the National Water Model and other large-scale hydrologic systems, significantly improving their accuracy and reliability in streamflow and storage simulations.

---

## Overview of gdromops

gdromops is a lightweight Python package that exposes pre-trained GDROM reservoir operation rules in a simple, reproducible interface. Its core class, RuleEngine, loads rule sets (by GRanD_ID or local reservoir ID) and evaluates operation logic to produce daily releases and updated storage. The package bundles rule modules, decision-tree conditions, and a small PDSI dataset so users can run realistic, data-driven reservoir simulations without external preprocessing.

Using the package is straightforward: instantiate RuleEngine for a reservoir, provide daily inflow and either observed storage or an initial storage value, and call the simulation methods (single-day or multi-day). Optional PDSI inputs can be supplied directly or looked up automatically from latitude/longitude. Output is returned as a pandas DataFrame that includes input fields (inflow, PDSI, DOY) and model results (simulated_release, simulated_storage), allowing immediate analysis or downstream processing.

gdromops is designed for integration into large-scale hydrologic and routing systems. In routing frameworks (for example, t-route or NWM-style models) each reservoir node can call RuleEngine with upstream inflows and state to obtain releases for downstream propagation. The API supports both observed-storage and data-limited modes, and its compact rule storage and vectorized simulation paths enable batch processing or parallel execution across many reservoirs.

The package emphasizes interoperability and minimal friction: ID crosswalks, clear inputs/outputs, and a small dependency footprint make it easy to embed gdromops into operational or research workflows. Users can therefore replace simplistic level-pool or fixed-curve approaches with GDROM-derived behavior, improving simulated streamflow and storage dynamics across large domains.


## Acknowledgements

This data product was supported by the Cooperative Institute for Research to Operations in Hydrology (CIROH) with funding under award NA22NWS4320003 from the NOAA Cooperative Institute Program. The statements, findings, conclusions, and recommendations are those of the author(s) and do not necessarily reflect the opinions of NOAA. 

---

## References

- Poff, N. & Allan, J. David & Bain, Mark & Karr, James & Prestegaard, Karen & Richter, Brian & Sparks, Richard & Stromberg, Juliet. (1997). The Natural Flow Regime: A Paradigm for River Conservation and Restoration. Bioscience. 47. 
- Vora, A, XM Cai, Y. Chen, and D. Li (2024) Coupling reservoir operation and rainfall-runoff processes for streamflow simulation in watersheds, Wat. Resour. Res. 60(6), doi:10.1029/2023WR035703. 
- Zheng, Z., X. Cai, Y. Chen (2025). GDROM v2: An Inventory of Operation Variables Time Series and Rules for 2,017 Large Reservoirs across the CONUS, HydroShare, https://doi.org/10.4211/hs.5293674cb83b4ec698db0eb4777467b8
- Cosgrove, Brian, David Gochis, Trey Flowers, Aubrey Dugger, Fred Ogden, Tom Graziano, Ed Clark et al; 2024. “ NOAA's National Water Model: Advancing Operational Hydrology Through Continental-scale Modeling.” JAWRA Journal of the American Water Resources Association 60 (2): 247–272. https://doi.org/10.1111/1752-1688.13184
- Chen, Y., Li, D., Zhao, Q. & Cai, X. Developing a generic data-driven reservoir operation model. Adv. Water Resour. 167, 104274 (2022).
- Li, D., Y. Chen, X. Cai, Q. Zhao (2023). Data-driven Reservoir Operation Rules for 450+ Reservoirs in Contiguous United States, HydroShare, https://doi.org/10.4211/hs.63add4d5826a4b21a6546c571bdece10
- Zihan Zheng1, Ximing Cai1, Linshui Zhang1, James Li1, Yanan Chen1,2 GDROM v2: An Inventory of Operation Variables Time Series and Rules for 2,017 Large Reservoirs across the CONUS. Scientific Data, second round review
- Zheng, Z., X. Cai, Y. Chen (2025). GDROM v2: An Inventory of Operation Variables Time Series and Rules for 2,017 Large Reservoirs across the CONUS, HydroShare, https://doi.org/10.4211/hs.5293674cb83b4ec698db0eb4777467b8


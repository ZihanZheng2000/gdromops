
import re

def build_ct_function_from_text(grand_id, text):
    func_name = f"CT_{grand_id}"
    lines = text.splitlines()
    body = [f"def {func_name}(inflow, pdsi, doy, storage):"]
    for line in lines:
        s = line.strip()
        if not s or not s.lower().startswith("if"):
            continue
        s = (s.replace("Inflow","inflow")
               .replace("PDSI","pdsi")
               .replace("DOY","doy")
               .replace("Storage","storage"))
        m = re.search(r"then\s+module:\s*(\d+)", s, flags=re.IGNORECASE)
        if m:
            module = m.group(1)
            condition = s.split("then")[0].strip()
            body.append(f"    {condition}:")
            body.append(f"        return {module}")
    body.append("    return None")
    ns = {}
    exec("\n".join(body), ns)
    return ns[func_name]

def build_module_function_from_text(grand_id, module_id, text):
    func_name = f"module_{grand_id}_{module_id}"
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    body = [f"def {func_name}(inflow, storage):"]
    for s in lines:
        if s.startswith("Release =") or s.startswith("Release="):
            expr = s.split("=",1)[1].strip()
            expr = (expr.replace("Inflow","inflow")
                        .replace("Storage","storage"))
            body.append(f"    return {expr}")
        elif "then Release:" in s:
            cond_part, val_part = s.split("then Release:")
            condition = cond_part.strip()[3:].strip()  # drop leading 'if '
            condition = (condition.replace("Inflow","inflow")
                                   .replace("Storage","storage"))
            value = val_part.strip()
            body.append(f"    if {condition}:")
            body.append(f"        return {value}")
        elif s.startswith("Release:"):
            val = s.split(":",1)[1].strip()
            body.append(f"    return {val}")
    body.append("    return None")
    ns = {}
    exec("\n".join(body), ns)
    return ns[func_name]

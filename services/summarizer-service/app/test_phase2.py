from app.nlp_engine import MedicalUnderwritingEngine
import json

def test():
    engine = MedicalUnderwritingEngine()
    
    # Test Case 1: Green Risk
    green_text = "Patient admitted for acute appendicitis. Appendectomy performed. Post-operative period was uneventful. Discharged in stable condition. No chronic history."
    
    # Test Case 2: Yellow Risk
    yellow_text = "Patient presented with dizziness. Diagnosed with essential hypertension. BP 160/100. Patient is overweight. Started on Amlodipine 5mg. Advised weight reduction."
    
    print("\n--- Testing GREEN Condition ---")
    print(json.dumps(engine.analyze_clinical_risk(green_text), indent=2))
    
    print("\n--- Testing YELLOW Condition ---")
    print(json.dumps(engine.analyze_clinical_risk(yellow_text), indent=2))

if __name__ == "__main__":
    test()
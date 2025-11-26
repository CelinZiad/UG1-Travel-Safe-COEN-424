import os
from typing import Optional

try:
    from transformers import pipeline
    HF_AVAILABLE = True
except ImportError:
    HF_AVAILABLE = False
    pipeline = None


class SafetyAdvisor:
    """AI-powered safety advice generator using Hugging Face models."""

    def __init__(self, model_name: Optional[str] = None):
        """
        Initialize the safety advisor with a text generation model.
        Uses a small model suitable for deployment on EC2.
        """
        self.model_name = model_name or os.getenv(
            "HF_MODEL",
            "distilgpt2"
        )
        self._generator = None
        self._model_loaded = False

    @property
    def generator(self):
        """Lazy load the model to avoid startup delays."""
        if not HF_AVAILABLE:
            return None
        if self._generator is None and not self._model_loaded:
            try:
                self._generator = pipeline(
                    "text-generation",
                    model=self.model_name,
                    max_length=150,
                    truncation=True
                )
            except Exception:
                self._generator = None
            self._model_loaded = True
        return self._generator

    def get_advice(self, area_name: str, safety_score: float,
                   crime_count: int, accident_count: int) -> str:
        """Generate safety advice for a specific area."""
        if safety_score >= 70:
            level = "safe"
            base_advice = "This area has good safety metrics."
        elif safety_score >= 40:
            level = "moderate"
            base_advice = "This area has moderate safety levels. Stay aware of your surroundings."
        else:
            level = "caution"
            base_advice = "Exercise caution in this area, especially during late hours."

        prompt = f"Safety advice for {area_name}: "

        if self.generator is not None:
            try:
                result = self.generator(prompt, max_new_tokens=50, do_sample=True, temperature=0.7)
                generated = result[0]["generated_text"]
                advice_part = generated[len(prompt):].strip()

                if advice_part and len(advice_part) > 10:
                    full_advice = f"{base_advice} {advice_part}"
                    return full_advice[:500]
            except Exception:
                pass

        return self._get_template_advice(level, area_name, crime_count, accident_count)

    def _get_template_advice(self, level: str, area_name: str,
                             crime_count: int, accident_count: int) -> str:
        """Generate template-based advice as fallback."""
        templates = {
            "safe": (
                f"{area_name} is considered a safe area based on recent data. "
                f"With {crime_count} reported incidents and {accident_count} traffic accidents "
                "in the last period, this area maintains good safety standards. "
                "Standard precautions are still recommended."
            ),
            "moderate": (
                f"{area_name} shows moderate safety levels. "
                f"There were {crime_count} crime incidents and {accident_count} accidents reported recently. "
                "Be mindful of your surroundings, especially at night. "
                "Avoid poorly lit areas and keep valuables secure."
            ),
            "caution": (
                f"{area_name} requires extra caution. "
                f"Recent data shows {crime_count} crime incidents and {accident_count} traffic accidents. "
                "Consider traveling with others, especially after dark. "
                "Stay on main roads and avoid isolated areas. "
                "Keep emergency contacts readily available."
            )
        }
        return templates.get(level, templates["moderate"])

    def get_route_recommendation(self, overall_score: float, num_areas: int) -> str:
        """Generate a recommendation for a route based on overall safety."""
        if overall_score >= 70:
            return (
                f"This route through {num_areas} areas appears safe. "
                "Traffic and crime levels are below average for the city. "
                "Proceed normally while maintaining standard awareness."
            )
        elif overall_score >= 40:
            return (
                f"This route passes through {num_areas} areas with varying safety levels. "
                "Some sections may require extra attention. "
                "Consider traveling during daylight hours if possible."
            )
        else:
            return (
                f"This route through {num_areas} areas has elevated risk indicators. "
                "Consider an alternative route if available. "
                "If this route is necessary, travel during busy hours and stay alert."
            )

    def answer_question(self, question: str) -> str:
        """Answer a natural language safety question."""
        question_lower = question.lower()

        if "safest" in question_lower:
            return (
                "The safest areas in Montreal typically include residential neighborhoods "
                "like Outremont, Westmount, and parts of Cote-des-Neiges. "
                "Use the /scores/latest endpoint to see current safety rankings."
            )

        if "dangerous" in question_lower or "avoid" in question_lower:
            return (
                "Areas with lower safety scores should be approached with caution, "
                "especially during late hours. Check the /areas endpoint for specific "
                "area scores and plan your route accordingly."
            )

        if "night" in question_lower or "evening" in question_lower:
            return (
                "During nighttime hours, stick to well-lit main streets and "
                "consider using the /analyze-route endpoint to check your planned path. "
                "Areas with scores below 50 warrant extra caution after dark."
            )

        if "score" in question_lower or "how" in question_lower:
            return (
                "Safety scores range from 0-100, with higher scores indicating safer areas. "
                "Scores are calculated based on crime incidents and traffic accidents. "
                "Green (70+) is safe, yellow (40-69) is moderate, red (below 40) requires caution."
            )

        if self.generator is not None:
            try:
                prompt = f"Montreal safety question: {question}\nAnswer: "
                result = self.generator(prompt, max_new_tokens=80, do_sample=True, temperature=0.7)
                generated = result[0]["generated_text"]
                answer = generated[len(prompt):].strip()

                if answer and len(answer) > 20:
                    return answer[:400]
            except Exception:
                pass

        return (
            "For specific safety information, use the /areas and /scores endpoints. "
            "You can also check individual areas with /areas/{area_id} or analyze "
            "a route with /analyze-route."
        )

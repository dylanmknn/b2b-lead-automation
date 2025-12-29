"""Agent 3: Full Email Sequence Generator with Claude API

Generates complete cold email sequence using the "Hiring Signal" angle:
- Email 1: Hook (hiring observation) + problem + social proof + soft CTA
- Email 2: Bump with different angle (time saved)
- Email 3: Breakup email

Output structure:
{
    "subject_line": "2 words, lowercase, internal-sounding",
    "email_1": "Main email (~60-80 words)",
    "email_1_ps": "P.S. line (personalized)",
    "email_2": "Follow-up bump (~40-50 words)",
    "email_3": "Breakup email (~30-40 words)"
}
"""

import anthropic
import json
from config.settings import settings


class Personalizer:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

    def generate_intro(self, lead):
        """Legacy method - now calls generate_full_sequence and returns email_1"""
        result = self.generate_full_sequence(lead)
        return result.get("email_1", self._fallback_intro(lead))

    def generate_first_line(self, lead):
        """Alias for generate_intro for compatibility"""
        return self.generate_intro(lead)

    def _fallback_intro(self, lead):
        """Fallback if generation fails"""
        job_title = lead.get("job_title", "commercial")
        return f"Vous recrutez un {job_title} - dans 30 jours il enverra 1,000 emails/jour. Votre infrastructure est-elle prete?"

    def generate_full_sequence(self, lead):
        """
        Generate complete email sequence using the Hiring Signal angle.

        Returns dict with:
        - subject_line
        - email_1, email_1_ps
        - email_2
        - email_3
        """

        company_name = lead.get("company_name", "Unknown")
        job_title = lead.get("job_title", "commercial")
        first_name = lead.get("first_name", "")
        last_name = lead.get("last_name", "")
        title = lead.get("title", "")

        prompt = f"""Tu es un expert en cold email B2B. Tu ecris des emails courts, directs, sans bullshit.

CONTEXTE:
- Entreprise: {company_name}
- Poste recrute: {job_title}
- Contact: {first_name} {last_name}, {title}

TON OFFRE (MilleMail):
1. Infrastructure cold email automatisee + workflows de lead scraping autonomes
2. Pour entreprises B2B
3. Volume reparti horizontalement sur plusieurs inboxes/domaines (1000+ emails/jour), AI scraping, personnalisation AI, SPF DKIM DMARC spintax etc
4. Cold outreach = facon la moins chere et plus scalable d'atteindre ton ICP
5. Tu as aide d'autres B2B a scaler leur outbound et generer plus de meetings sur autopilot
6. ROI: temps gagne + scalabilite + argent genere

ANGLE: "Hiring Signal"
- Tu vois qu'ils recrutent un poste commercial/sales/growth
- En 30 jours ce nouvel employe enverra 1000+ cold emails/jour
- Question: leur infrastructure est-elle prete?
- Tu proposes de resoudre ce probleme

REGLES STRICTES:
- Email 1: 60-80 mots MAX (sans compter le PS)
- Email 2: 40-50 mots MAX
- Email 3: 30-40 mots MAX
- Sujet: 2 mots, minuscules, style interne (pas commercial)
- Ton: direct, professionnel, leger, pas vendeur
- PAS de liens
- PAS de "J'espere que vous allez bien"
- PAS de questions rhetoriques inutiles
- Soft CTA uniquement ("Ca vaut le coup d'en parler?" pas "Reservez un appel")
- Tutoiement OK si naturel
- Ecris en francais

SPINTAX OBLIGATOIRE:
Tu DOIS utiliser du spintax pour creer des variations. Format: {{option1|option2|option3}}
Chaque email doit avoir 5-8 spintax minimum pour garantir des variations uniques.

Exemples de spintax:
- {{Je vois|J'ai remarque|Je note}} que {{vous recrutez|vous cherchez|vous embauchez}}
- {{Dans 30 jours|D'ici un mois|Tres bientot}}, {{cette personne|ce nouveau recrue|votre nouvel employe}}
- {{Ca vaut le coup d'en parler|On en discute|Interesse d'en savoir plus}}?
- {{infrastructure|systeme|setup}} {{prete|en place|operationnelle}}

IMPORTANT: Le spintax doit sonner naturel dans TOUTES les combinaisons possibles.

STRUCTURE EMAIL 1:
1. Hook: Reference au poste recrute (preuve que tu as fait tes recherches)
2. Probleme: Infrastructure pas prete = emails en spam
3. Solution: Ce que tu fais (1 phrase)
4. Social proof: Resultats (1 phrase)
5. Soft CTA
6. PS: Observation personnalisee ou note legere

STRUCTURE EMAIL 2 (3 jours apres):
- Bump court
- Angle different: temps gagne ou scalabilite
- Soft CTA

STRUCTURE EMAIL 3 (7 jours apres):
- Dernier email
- Breakup style
- Laisse la porte ouverte

REPONDS EN JSON VALIDE UNIQUEMENT (pas de markdown, pas de backticks):
{{"subject_line": "...", "email_1": "...", "email_1_ps": "PS: ...", "email_2": "...", "email_3": "..."}}"""

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}],
            )

            response_text = message.content[0].text.strip()

            # Parse JSON response
            try:
                result = json.loads(response_text)
                print(f"  Generated sequence for {company_name}")
                return result
            except json.JSONDecodeError:
                # Try to extract JSON from response
                import re

                json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                    print(f"  Generated sequence for {company_name}")
                    return result
                else:
                    print(f"  Failed to parse JSON for {company_name}, using fallback")
                    return self._fallback_sequence(lead)

        except Exception as e:
            print(f"  Error generating sequence for {company_name}: {str(e)}")
            return self._fallback_sequence(lead)

    def _fallback_sequence(self, lead):
        """Fallback email sequence if generation fails - includes spintax"""
        job_title = lead.get("job_title", "commercial")
        company_name = lead.get("company_name", "votre entreprise")

        return {
            "subject_line": "{infrastructure|setup} email",
            "email_1": f"{{Je vois|J'ai remarque|Je note}} que {company_name} {{recrute|cherche|embauche}} un {job_title}. {{Dans 30 jours|D'ici un mois}}, {{cette personne|ce nouvel employe}} enverra 1000+ cold emails {{par jour|quotidiennement}}. {{La question|Le truc}}: votre {{infrastructure|systeme}} est-{{elle prete|il en place}} pour ca sans finir en spam? On a {{monte|construit}} ce type de systeme pour d'autres {{boites|entreprises}} B2B - ils generent {{20+|une vingtaine de}} meetings/mois en autopilot {{maintenant|aujourd'hui}}. {{Ca vaut le coup d'en parler|On en discute|Interesse}}?",
            "email_1_ps": "PS: {{Pas de pression|Sans pression}}, {{juste curieux|je me demandais}} comment vous {{gerez|faites}} ca {{aujourd'hui|actuellement}}.",
            "email_2": "{{Je reviens|Je rebondis}} sur mon {{dernier message|email precedent}}. Si {{ton equipe|tes commerciaux}} {{passe|passent}} plus de temps a prospecter qu'a closer, {{y'a un probleme|c'est un signal}}. {{On peut en parler|Un call de}} 10 min?",
            "email_3": "{{Dernier message|Derniere relance}} de ma part. Si {{c'est pas le bon moment|le timing est mauvais}}, {{pas de souci|aucun probleme}}. {{La porte reste ouverte|Je reste dispo}} si ca {{devient pertinent|t'interesse plus tard}}.",
        }

    def generate_millemail_sequence(self, lead, sender_name="Dylan"):
        """
        Generate MilleMail-specific email sequence using 4 pre-written versions.
        Randomly selects one of 4 versions (A/B/C/D) for variation.

        Returns dict with:
        - subject_line
        - email_1, email_1_ps (optional)
        - email_2
        - email_3
        """
        import random

        company_name = lead.get("company_name", "Unknown")
        first_name = lead.get("first_name", "")

        # Greeting - use first name if available, otherwise skip
        greeting = f"{first_name},\n\n" if first_name else ""

        # 4 PRE-WRITTEN SEQUENCES - Random rotation
        SEQUENCES = {
            "version_a": {
                "name": "Problem-First",
                "subject_line": "infra + deals perdus?",
                "email_1": f"""{greeting}95% des boîtes qui scaleup leur outbound utilisent des infras partagées (Lemlist, Instantly). Résultat : scores spam partagés, taux d'inbox qui chutent.

On construit des infras dédiées que vous possédez. Scraping de leads + 1000 emails/jour en inbox. Autopilot, RGPD compliant.

Je peux vous envoyer l'audit gratuit qu'on utilise pour identifier où vous perdez des deals ?""",
                "email_2": """Lemlist/Instantly = infrastructure partagée = vous héritez des scores spam de 500+ autres boîtes.

On construit la vôtre. Vous possédez tout. Setup 24h.

L'audit ?""",
                "email_3": """Dernier message.

Si vos taux de réponse outbound sont en dessous de 10%, c'est probablement l'infra.

Audit gratuit disponible si ça vous intéresse.""",
            },
            "version_b": {
                "name": "Direct ROI",
                "subject_line": "infra + leads auto?",
                "email_1": f"""{greeting}Vous recrutiez donc vous scalez.

Si je pouvais mettre 10 RDV qualifiés en plus sur votre agenda le mois prochain en autopilot, comme pour une boîte comme la vôtre, ça vaudrait 5 min de discussion ?

Infra email dédiée (vous êtes propriétaire) + scraping de leads + séquences automatisées. Setup en 24h, 1000 emails/jour en inbox, RGPD compliant.

Intéressé ?""",
                "email_2": """Une boîte comme la vôtre génère 12-15 RDV/mois avec notre infra depuis 3 mois.

Même setup, même process, même résultats.

5 min ?""",
                "email_3": """Pas de souci si le timing n'est pas bon.

Quand vous voudrez scaler l'outbound sans brûler votre réputation, on sera là.""",
            },
            "version_c": {
                "name": "Authority",
                "subject_line": "95% inbox rate?",
                "email_1": f"""{greeting}La plupart des agences cold email utilisent des infras partagées. On fait l'inverse : infra dédiée que VOUS possédez.

Résultats clients :
- 95% taux d'inbox (vs 40-60% en shared)
- 1000 emails/jour en autopilot
- Leads scrapés et qualifiés automatiquement
- Setup 24h, RGPD native

On peut parler 5 min de comment ça marche pour votre cas ?""",
                "email_2": """La différence entre 40% et 95% de taux d'inbox sur 1000 emails/jour = 550 prospects de plus qui voient votre message.

Par jour.

Ça change quoi pour vous ?""",
                "email_3": """Dernière tentative.

Setup en 24h, vous possédez l'infra, leads en autopilot, RGPD compliant.

Ou vous continuez avec votre setup actuel. Les deux fonctionnent.""",
            },
            "version_d": {
                "name": "Pattern Interrupt",
                "subject_line": "lemlist ou instantly?",
                "email_1": f"""{greeting}Question rapide : vous utilisez Lemlist, Instantly ou autre chose pour votre outbound ?

Si oui, vous partagez votre réputation d'envoi avec des centaines d'autres boîtes. Leurs problèmes de spam = vos problèmes de spam.

On monte des infras que vous possédez à 100%. Leads en autopilot, 1000 emails/jour qui atterrissent en inbox, RGPD compliant, setup 24h.

Curieux de voir la différence ?""",
                "email_2": """Infra partagée = vous payez pour brûler votre réputation d'envoi.

Infra dédiée = vous payez pour la construire.

Quelle approche fait plus de sens pour scaler ?""",
                "email_3": """Je ferme la boucle ici.

Si vous voulez voir comment fonctionne une vraie infra dédiée vs. du shared, faites-moi signe.

Sinon bonne continuation.""",
            },
        }

        # Random version selection
        version_key = random.choice(list(SEQUENCES.keys()))
        selected = SEQUENCES[version_key]

        print(f"  Using {selected['name']} (version {version_key}) for {company_name}")

        return {
            "subject_line": selected["subject_line"],
            "email_1": selected["email_1"],
            "email_1_ps": "",  # No PS in these versions
            "email_2": selected["email_2"],
            "email_3": selected["email_3"],
        }

    def _fallback_millemail_sequence(self, lead):
        """Fallback MilleMail email sequence if generation fails - includes spintax"""
        company_name = lead.get("company_name", "votre entreprise")

        return {
            "subject_line": "recrutement + outbound?",
            "email_1": f"{{Je vois|J'ai vu|Je note}} que vous {{recrutez|embauchez|cherchez}} chez {company_name}. {{Vous scalez|Vous montez|Vous developpez}} votre {{cold email outbound|prospection email|outbound email}} aussi? {{Vous avez l'infra|Infrastructure en place|Setup pret}} pour {{monter a|scaler a|atteindre}} {{1000 emails/jour|1K/jour|volume 1000+}}? {{Lead scraping|Scraping leads|Data acquisition}} {{automatise|en auto|automatique}}? {{Personnalisation|Customisation|Perso}} {{automatisee|auto|en auto}}? {{Deliverabilite geree|Warmup + rotation|Infra deliverabilite}} (warmup, rotation domaines, spintax)? La plupart des {{boites|entreprises|teams}} {{bloquent|plafonnent|stagnent}} a {{100-200/jour|100/jour|petit volume}} - {{infra pas la|infrastructure limite|pas le setup}}. On a {{monte|construit|deploye}} des systemes {{1000+/jour|volume industriel|1K+ quotidien}} {{full auto|100% automatise|en autopilot}} pour d'autres B2B - {{20+ meetings/mois|une vingtaine de rdv|20+ rendez-vous mensuels}}. {{Ca vaut le coup|Interesse|On en parle}}?",
            "email_1_ps": "P.S. {{Sans pitch|Pas de pitch}}, {{juste curieux|je me demandais}} {{quel est votre setup|ou vous en etes|quelle infra}} {{actuellement|aujourd'hui|en ce moment}}.",
            "email_2": "{{Vous saviez|Vous savez|Info}} que {{cold email|l'outbound email|prospection email}} = {{canal le plus scalable|meilleur ROI|opportunite #1}} en {{2024|cette annee|maintenant}} {{quand bien fait|si bien execute|avec bonne infra}}? {{LinkedIn ads|Pub LinkedIn|Ads}}, events, {{cold calling|appels a froid|prospection tel}} = {{cher|couteux|budget eleve}} + {{pas scalable|limite|plafond bas}}. Cold email {{bien fait|avec infra solide|execute correctement}} (automation + deliverabilite + volume) = {{best opportunity|meilleur canal|#1 acquisition}} B2B. La plupart {{le font mal|echouent|spam}} (pas d'infra, {{volume faible|trop peu|50/jour max}}). {{Quand bien fait|Avec bonne execution|Setup correct}} = {{meilleur canal|imbattable|ROI imbattable}}, point. {{Interesse|Ca vaut le coup|On en parle}}?",
            "email_3": "{{Vos concurrents|La concurrence|Vos competitors}} {{font|font deja|executent}} du cold outbound a {{1000+/jour|volume industriel|1K+ quotidien}}. Vous? {{Combien votre equipe|Votre team fait combien|Volume actuel}}? {{50/jour|100/jour|200/jour}}? {{Pendant que|Tant que}} vous {{hesitez|attendez|reflechissez}}, {{ils prennent|ils gagnent|gap se creuse}} des parts de marche. {{Porte ouverte|Dispo|Contact ouvert}} si {{ca devient priorite|vous voulez scaler|interet evolue}}.",
        }

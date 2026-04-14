from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlparse

from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama
from tenacity import retry, stop_after_attempt, wait_exponential

SKILLS_CATALOG = (
    "1:Access Network Design & Planning|2:Access Network Operations|3:Accounting Information Systems|"
    "4:Accounting Standards|5:Active Infrastructure Rollout|6:Agile / Scrum Project Management|"
    "7:Analytical Thinking|8:Application & Content Development|9:B2B Customer Care / Digital Care|"
    "10:B2B Product / Portfolio Strategy Development|11:B2B Product Pricing & Offering|"
    "12:B2B Solution Selling|13:B2C Sales|14:Banking & Financial Compliance Regulations|"
    "15:Bidding & Tendering Process|16:Big Data Management|17:Brand Management|18:Brand Marketer|"
    "19:Business Case Development|20:Business English|21:Business Modeling|"
    "22:Business Process Management (BPM)|23:Business Strategist|24:Campaign Management|"
    "25:Capturing the Voice of the Customer (VoC)|26:Cash Management|"
    "27:Category Development & Management|28:Category Specific Knowledge|"
    "29:Change & Release Management|30:Cloud Based Services|31:Complaint Management & Resolution|"
    "32:Computer & Office Technology|33:Concept to Market / Go to Market|34:Consumer Analytics|"
    "35:Contact Center Tools|36:Content Design & Production|37:Continuous Improvement (CI)|"
    "38:Contract Support|39:Converged Products & Services|40:Core Network Design & Planning|"
    "41:Corporate Affairs|42:Corporate Social Responsibility|43:Cost Accounting & Management|"
    "44:Creative Copywriting|45:Creative Thinking|46:Credit & Collection Process|"
    "47:Critical Thinking|48:Cross-Selling|49:Customer Communication|"
    "50:Customer Experience Analytics|51:Customer Experience Practices|52:Customer Journey Mapping|"
    "53:Customer Value Management|54:Customs Clearance Process|55:Cyber Security|"
    "56:Data Center Design & Planning|57:Data Center Operation & Maintenance|"
    "58:Data Modeling & Pipelining|59:Data Monetization|60:Design Management|61:Device Management|"
    "62:Digital Analytics|63:Digital Business Development|64:Digital Care|"
    "65:Digital Channels Management (Commercial Aspects)|"
    "66:Digital Channels Management (Technical Aspects)|67:Digital Content Management|"
    "68:Digital CX Technologies & Trends|69:Digital Procurement|70:Digital Technology Knowledge|"
    "71:Distribution Logistics|72:Documents & Records Management|73:E2E Audit & Reconciliation|"
    "74:Employee Engagement|75:Employee Performance Management|76:Enterprise Architecture|"
    "77:Enterprise Telephony Management|78:Entrepreneurial Mindset|"
    "79:Events & Sponsorship Management|80:Expats Integration & Management|"
    "81:Facilities Operations and Maintenance|82:Financial Acumen|83:Financial Analysis|"
    "84:Financial Reporting & Compliance|85:Fixed Assets Management|86:Forklift Operation|"
    "87:Fraud Management|88:Funding & Banking Relations|89:Geomarketing|"
    "90:Governance Risk & Control|91:Government Relations|92:HR Analytics|"
    "93:Incentives Program Management|94:Incident Management|"
    "95:Information Security Compliance & Audit|96:Information Security Governance|"
    "97:Information Security Operation & Support|98:Information Security Risk Management|"
    "99:Information Security Systems & Tools|100:Insurance Claim Processing|"
    "101:Insurance Planning & Policies Development|102:Integrated Marketing Communication|"
    "103:Integrated Network Dimensioning|104:Internal Audit Planning & Execution|"
    "105:Internal Communication Management|106:Internal Control|"
    "107:International Professional Practices Framework (IPPF)|108:Internet of Things (and M2M)|"
    "109:Inventory Management & Optimization|110:Investment Evaluation & Management|"
    "111:IT Platforms Operations|112:Job Analysis - Design & Evaluation|"
    "113:Key Account Management|114:Knowledge Base Management|"
    "115:Labor Law & Employment Legislation|116:Lobbying|"
    "117:Local Digital Laws & Regulations|118:Local Language Knowledge|"
    "119:Logistics & Warehousing|120:Machine Learning / Artificial Intelligence|"
    "121:Management Reporting|122:Market Research & Study|123:Market Segmentation|"
    "124:Mechanical - Electrical & Plumbing Knowledge|125:Media Planning & Management|"
    "126:Media Technologies|127:Meeting Administration|128:Message Handling|"
    "129:MFS Adjacent Services & Products|130:MFS Ecosystem & Trends|"
    "131:Monitoring & Measuring Communication Effectiveness|132:Negotiation|"
    "133:Network Analytics|134:Network Monitoring|135:Network Operations Center (NOC)|"
    "136:Network Optimization|137:Network Performance Management|138:Network Testing|"
    "139:Network Virtualization (NV)|140:Online / Digital Marketing|141:Online / Digital Sales|"
    "142:Organizational Structuring|143:Outdoor Advertising Management|"
    "144:Partnership Engagement - Execution & Monitoring|145:Passive Infrastructure Deployment|"
    "146:Passive Network Operations|147:Payroll Process Management|"
    "148:Performance Dashboard Design & Development|149:Performance Management & Reporting|"
    "150:Performance Measurement & Reporting|151:Planning & Budgeting|152:Problem Solving|"
    "153:Product / Portfolio Strategy Development|154:Product Development|"
    "155:Product Portfolio & Lifecycle Management|156:Project Management|"
    "157:Protocol Service Knowledge|158:Public Relations|"
    "159:QMS Implementation & Maintenance|160:Regulatory Economics|"
    "161:Request to Pay Process (R2P)|162:Retail Sales Operations|"
    "163:Retention & Loyalty Program Management|164:Revenue Assurance Systems & Tools|"
    "165:Revenue Cycle Management|166:Revenue Leakage Control|167:Reward Management|"
    "168:Risk Appetite Framework Management|169:Risk Identification & Assessment|"
    "170:Risk Management|171:Risk Management Policy & Procedures|"
    "172:Risk Response & Reporting|173:Route Planning & Traffic Regulations|"
    "174:Safety Management|175:Sales & Distribution Analytics|176:Sales Automation|"
    "177:Sales Fulfillment|178:Sales Training|179:Scarce Resources Management|"
    "180:Security Planning|181:Service Assurance & Quality|"
    "182:Service Configuration & Activation Process|183:Service Delivery Networks|"
    "184:Service Quality Monitoring & Compliance|185:Service-Orientated IT|"
    "186:Services Integration|187:Shared Services Management|188:Smart Pricing|"
    "189:Social Media Management|190:Statistics|191:Strategy Formulation|"
    "192:Strategy Implementation|193:Succession Planning|"
    "194:Supplier Negotiation & Deal Closing|195:Suppliers & Contracts Management|"
    "196:Supply Market Analysis|197:Systems Integration|198:Talent Assessment|"
    "199:Talent Capability Building|200:Talent Market Intelligence & Acquisition|"
    "201:Tax Audit & Planning|202:Tax Return Preparation|203:Taxation Law|"
    "204:Technical Aspects of Wholesale|205:Technical Specifications Development|"
    "206:Technical Writing and Reporting|207:Telecom Market & Industry Knowledge|"
    "208:Telecom Regulatory Policy|209:Trade Marketing|"
    "210:Training Management & Facilitation|211:Transactional Accounting & Closing|"
    "212:Translation & Interpretation|213:Transport Network Design & Planning|"
    "214:Transportation & Fleet Management|215:Travel Planning & Assistance|"
    "216:Treasury Policies & Risk|217:Troubleshooting & Technical Problem Solving|"
    "218:User Acceptance Testing (UAT)|219:Website Management|"
    "220:Wholesale Access Regulations|221:Workforce Management"
)

# Canonical catalog with categories (source of truth for skill approximation).
# Format: "id,name,category" CSV, one row per line.
SKILLS_CATALOG_CSV = """id,name,category
1,Access Network Design & Planning,Network
2,Access Network Operations,Network
3,Accounting Information Systems,Finance
4,Accounting Standards,Finance
5,Active Infrastructure Rollout,Network
6,Agile / Scrum Project Management,Project Management
7,Analytical Thinking,Soft Skills
8,Application & Content Development,Digital
9,B2B Customer Care / Digital Care,Customer Experience
10,B2B Product / Portfolio Strategy Development,Business Strategy
11,B2B Product Pricing & Offering,Commercial
12,B2B Solution Selling,Sales
13,B2C Sales,Sales
14,Banking & Financial Compliance Regulations,Finance
15,Bidding & Tendering Process,Procurement
16,Big Data Management,Data
17,Brand Management,Marketing
18,Brand Marketer,Marketing
19,Business Case Development,Business Strategy
20,Business English,Communication
21,Business Modeling,Business Strategy
22,Business Process Management (BPM),Operations
23,Business Strategist,Business Strategy
24,Campaign Management,Marketing
25,Capturing the Voice of the Customer (VoC),Customer Experience
26,Cash Management,Finance
27,Category Development & Management,Commercial
28,Category Specific Knowledge,Commercial
29,Change & Release Management,IT Operations
30,Cloud Based Services,Cloud
31,Complaint Management & Resolution,Customer Experience
32,Computer & Office Technology,IT
33,Concept to Market / Go to Market,Product Management
34,Consumer Analytics,Analytics
35,Contact Center Tools,Customer Experience
36,Content Design & Production,Content
37,Continuous Improvement (CI),Operations
38,Contract Support,Legal
39,Converged Products & Services,Telecom
40,Core Network Design & Planning,Network
41,Corporate Affairs,Corporate
42,Corporate Social Responsibility,Corporate
43,Cost Accounting & Management,Finance
44,Creative Copywriting,Content
45,Creative Thinking,Soft Skills
46,Credit & Collection Process,Finance
47,Critical Thinking,Soft Skills
48,Cross-Selling,Sales
49,Customer Communication,Customer Experience
50,Customer Experience Analytics,Customer Experience
51,Customer Experience Practices,Customer Experience
52,Customer Journey Mapping,Customer Experience
53,Customer Value Management,Customer Experience
54,Customs Clearance Process,Logistics
55,Cyber Security,Security
56,Data Center Design & Planning,Infrastructure
57,Data Center Operation & Maintenance,Infrastructure
58,Data Modeling & Pipelining,Data
59,Data Monetization,Data
60,Design Management,Design
61,Device Management,IT
62,Digital Analytics,Digital
63,Digital Business Development,Digital
64,Digital Care,Customer Experience
65,Digital Channels Management (Commercial Aspects),Digital
66,Digital Channels Management (Technical Aspects),Digital
67,Digital Content Management,Content
68,Digital CX Technologies & Trends,Customer Experience
69,Digital Procurement,Procurement
70,Digital Technology Knowledge,IT
71,Distribution Logistics,Logistics
72,Documents & Records Management,Operations
73,E2E Audit & Reconciliation,Finance
74,Employee Engagement,HR
75,Employee Performance Management,HR
76,Enterprise Architecture,IT
77,Enterprise Telephony Management,Telecom
78,Entrepreneurial Mindset,Soft Skills
79,Events & Sponsorship Management,Marketing
80,Expats Integration & Management,HR
81,Facilities Operations and Maintenance,Operations
82,Financial Acumen,Finance
83,Financial Analysis,Finance
84,Financial Reporting & Compliance,Finance
85,Fixed Assets Management,Finance
86,Forklift Operation,Logistics
87,Fraud Management,Risk
88,Funding & Banking Relations,Finance
89,Geomarketing,Marketing
90,Governance Risk & Control,Risk
91,Government Relations,Corporate
92,HR Analytics,HR
93,Incentives Program Management,HR
94,Incident Management,IT Operations
95,Information Security Compliance & Audit,Security
96,Information Security Governance,Security
97,Information Security Operation & Support,Security
98,Information Security Risk Management,Security
99,Information Security Systems & Tools,Security
100,Insurance Claim Processing,Finance
101,Insurance Planning & Policies Development,Finance
102,Integrated Marketing Communication,Marketing
103,Integrated Network Dimensioning,Network
104,Internal Audit Planning & Execution,Audit
105,Internal Communication Management,Corporate
106,Internal Control,Finance
107,International Professional Practices Framework (IPPF),Audit
108,Internet of Things (and M2M),Technology
109,Inventory Management & Optimization,Logistics
110,Investment Evaluation & Management,Finance
111,IT Platforms Operations,IT
112,Job Analysis - Design & Evaluation,HR
113,Key Account Management,Sales
114,Knowledge Base Management,IT
115,Labor Law & Employment Legislation,Legal
116,Lobbying,Corporate
117,Local Digital Laws & Regulations,Legal
118,Local Language Knowledge,Communication
119,Logistics & Warehousing,Logistics
120,Machine Learning / Artificial Intelligence,Data
121,Management Reporting,Finance
122,Market Research & Study,Marketing
123,Market Segmentation,Marketing
124,Mechanical - Electrical & Plumbing Knowledge,Engineering
125,Media Planning & Management,Marketing
126,Media Technologies,Digital
127,Meeting Administration,Operations
128,Message Handling,Communication
129,MFS Adjacent Services & Products,Telecom
130,MFS Ecosystem & Trends,Telecom
131,Monitoring & Measuring Communication Effectiveness,Marketing
132,Negotiation,Soft Skills
133,Network Analytics,Network
134,Network Monitoring,Network
135,Network Operations Center (NOC),Network
136,Network Optimization,Network
137,Network Performance Management,Network
138,Network Testing,Network
139,Network Virtualization (NV),Network
140,Online / Digital Marketing,Digital
141,Online / Digital Sales,Sales
142,Organizational Structuring,HR
143,Outdoor Advertising Management,Marketing
144,Partnership Engagement - Execution & Monitoring,Business Strategy
145,Passive Infrastructure Deployment,Network
146,Passive Network Operations,Network
147,Payroll Process Management,Finance
148,Performance Dashboard Design & Development,Analytics
149,Performance Management & Reporting,HR
150,Performance Measurement & Reporting,HR
151,Planning & Budgeting,Finance
152,Problem Solving,Soft Skills
153,Product / Portfolio Strategy Development,Product Management
154,Product Development,Product Management
155,Product Portfolio & Lifecycle Management,Product Management
156,Project Management,Project Management
157,Protocol Service Knowledge,Telecom
158,Public Relations,Marketing
159,QMS Implementation & Maintenance,Quality
160,Regulatory Economics,Legal
161,Request to Pay Process (R2P),Finance
162,Retail Sales Operations,Sales
163,Retention & Loyalty Program Management,Customer Experience
164,Revenue Assurance Systems & Tools,Finance
165,Revenue Cycle Management,Finance
166,Revenue Leakage Control,Finance
167,Reward Management,HR
168,Risk Appetite Framework Management,Risk
169,Risk Identification & Assessment,Risk
170,Risk Management,Risk
171,Risk Management Policy & Procedures,Risk
172,Risk Response & Reporting,Risk
173,Route Planning & Traffic Regulations,Logistics
174,Safety Management,Operations
175,Sales & Distribution Analytics,Sales
176,Sales Automation,Sales
177,Sales Fulfillment,Sales
178,Sales Training,Sales
179,Scarce Resources Management,Operations
180,Security Planning,Security
181,Service Assurance & Quality,Telecom
182,Service Configuration & Activation Process,Telecom
183,Service Delivery Networks,Telecom
184,Service Quality Monitoring & Compliance,Telecom
185,Service-Orientated IT,IT
186,Services Integration,IT
187,Shared Services Management,Operations
188,Smart Pricing,Commercial
189,Social Media Management,Marketing
190,Statistics,Data
191,Strategy Formulation,Business Strategy
192,Strategy Implementation,Business Strategy
193,Succession Planning,HR
194,Supplier Negotiation & Deal Closing,Procurement
195,Suppliers & Contracts Management,Procurement
196,Supply Market Analysis,Procurement
197,Systems Integration,IT
198,Talent Assessment,HR
199,Talent Capability Building,HR
200,Talent Market Intelligence & Acquisition,HR
201,Tax Audit & Planning,Finance
202,Tax Return Preparation,Finance
203,Taxation Law,Legal
204,Technical Aspects of Wholesale,Telecom
205,Technical Specifications Development,Engineering
206,Technical Writing and Reporting,Communication
207,Telecom Market & Industry Knowledge,Telecom
208,Telecom Regulatory Policy,Telecom
209,Trade Marketing,Marketing
210,Training Management & Facilitation,HR
211,Transactional Accounting & Closing,Finance
212,Translation & Interpretation,Communication
213,Transport Network Design & Planning,Network
214,Transportation & Fleet Management,Logistics
215,Travel Planning & Assistance,Operations
216,Treasury Policies & Risk,Finance
217,Troubleshooting & Technical Problem Solving,IT
218,User Acceptance Testing (UAT),IT
219,Website Management,Digital
220,Wholesale Access Regulations,Telecom
221,Workforce Management,HR
"""

STRUCTURE_PROMPT_TEMPLATE = """Extract ALL information from the CV below into valid JSON. No markdown, no commentary.
You MUST populate every field that exists in the CV. Do NOT return empty arrays if data is present.

Schema:
{{"contact":{{"name":"","email":"","phone":"","linkedin":"","location":""}},
"languages":[{{"language":"ENGLISH","proficiency":"B2"}}],
"education":[{{"institution":"","establishment":"","typeEducation":null,"dateGraduation":null}}],
"experience":[{{"role":"","company":"","location":"","startDate":"","endDate":"","description":""}}],
"certifications":[{{"title":"","issuer":"","issueDate":"","expiryDate":"","description":""}}],
"achievement":[{{"projectName":"","description":"","startDate":null,"endDate":null}}],
"skills":{{"technical":["Java","Python"],"soft":["Problem Solving"]}},
"summary":""}}

IMPORTANT:
- Extract EVERY work experience, project, skill, and language from the CV.
- technical skills: programming languages, frameworks, tools, databases.
- soft skills: leadership, communication, teamwork, etc.
- location = physical address/city, NOT email.
- typeEducation: LICENCE|MASTER|DOCTORAT|INGENIEUR|BTS|DUT|FORMATION_PROFESSIONNELLE or null.
- proficiency: A1|A2|B1|B2|C1|C2|NATIVE.
- dateGraduation: year as integer (e.g. 2023).
- Dates: DD/MM/YYYY. null for missing values. [] only for truly empty arrays.
- summary: 1-2 sentence professional summary.

CV TEXT:
{raw_text}
"""


@dataclass
class _CircuitState:
    fail_count: int = 0
    opened_at: float = 0.0


class CircuitBreaker:
    def __init__(self, fail_max: int = 3, reset_timeout: int = 60) -> None:
        self.fail_max = fail_max
        self.reset_timeout = reset_timeout
        self._state = _CircuitState()
        self._lock = threading.Lock()

    def is_open(self) -> bool:
        with self._lock:
            if self._state.fail_count < self.fail_max:
                return False
            if time.time() - self._state.opened_at >= self.reset_timeout:
                self._state.fail_count = 0
                self._state.opened_at = 0.0
                return False
            return True

    def on_success(self) -> None:
        with self._lock:
            self._state.fail_count = 0
            self._state.opened_at = 0.0

    def on_failure(self) -> None:
        with self._lock:
            self._state.fail_count += 1
            if self._state.fail_count >= self.fail_max:
                self._state.opened_at = time.time()


class OllamaClient:
    def __init__(self, model_name: str, base_url: str, timeout_seconds: int = 180) -> None:
        self._model_name = model_name
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._model = ChatOllama(model=model_name, base_url=base_url, timeout=timeout_seconds)
        self._breaker = CircuitBreaker(fail_max=3, reset_timeout=60)
        parsed = urlparse(self._base_url)
        self._api_host = parsed.hostname or "localhost"
        self._api_port = parsed.port or (443 if parsed.scheme == "https" else 11434)
        self._api_scheme = parsed.scheme or "http"
        self._conn: HTTPConnection | HTTPSConnection | None = None
        self._conn_lock = threading.Lock()

    @property
    def breaker_open(self) -> bool:
        return self._breaker.is_open()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    def call(self, prompt_template: ChatPromptTemplate, params: dict[str, str]) -> str:
        if self._breaker.is_open():
            raise RuntimeError("Circuit breaker is open for Ollama")
        chain = prompt_template | self._model
        try:
            llm_response = chain.invoke(params)
            content = llm_response.content if isinstance(llm_response.content, str) else str(llm_response.content)
            self._breaker.on_success()
            return content
        except Exception:
            self._breaker.on_failure()
            raise

    def _get_conn(self) -> HTTPConnection | HTTPSConnection:
        with self._conn_lock:
            if self._conn is not None:
                return self._conn
            if self._api_scheme == "https":
                conn = HTTPSConnection(self._api_host, self._api_port, timeout=self._timeout_seconds)
            else:
                conn = HTTPConnection(self._api_host, self._api_port, timeout=self._timeout_seconds)
            self._conn = conn
            return conn

    def _post_json(self, path: str, payload: dict) -> dict:
        body = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json", "Connection": "keep-alive"}
        conn = self._get_conn()
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            raw = resp.read().decode("utf-8")
        except Exception:
            with self._conn_lock:
                self._conn = None
            raise
        return json.loads(raw)

    def call_structured_cv(self, raw_text: str) -> str:
        if self._breaker.is_open():
            raise RuntimeError("Circuit breaker is open for Ollama")
        try:
            prompt = STRUCTURE_PROMPT_TEMPLATE.format(raw_text=raw_text)
            num_predict = int(os.getenv("OLLAMA_NUM_PREDICT", "1536"))
            num_thread = int(os.getenv("OLLAMA_LLAMA_NUM_THREAD", os.getenv("OLLAMA_NUM_THREAD", "4")))
            num_ctx = int(os.getenv("OLLAMA_NUM_CTX", "8192"))
            payload = {
                "model": self._model_name,
                "prompt": prompt,
                "format": "json",
                "stream": False,
                "options": {
                    "num_predict": num_predict,
                    "temperature": 0,
                    "top_p": 0.9,
                    "num_thread": max(1, num_thread),
                    "num_ctx": max(2048, num_ctx),
                },
            }
            obj = self._post_json("/api/generate", payload)
            if isinstance(obj, dict) and obj.get("error"):
                raise RuntimeError(f"Ollama error: {obj.get('error')}")
            content = str(obj.get("response", ""))
            if not content.strip():
                raise RuntimeError("Ollama returned empty response")
            self._breaker.on_success()
            return content
        except Exception:
            self._breaker.on_failure()
            raise

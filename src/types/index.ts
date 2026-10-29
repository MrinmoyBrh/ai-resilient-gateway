export interface AIJobPayload {
  jobId: string;
  tenantId: string;
  prompt: string;
  taskType?: 'incident_triage' | 'general';
}

export interface IncidentTriageOutput {
  severity: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
  rootCause: string;
  affectedService: string;
  recommendedAction: string;
}

export interface AIJobResult {
  jobId: string;
  content: IncidentTriageOutput | string;
  providerResolved: 'groq' | 'gemini';
  modelResolved: string;
  latencyMs: number;
  tokens: {
    prompt: number;
    completion: number;
  };
}

export interface Tenant {
  id: string;
  name: string;
  apiKey: string;
  tier: 'free' | 'pro' | 'enterprise';
  rpmLimit: number;
}
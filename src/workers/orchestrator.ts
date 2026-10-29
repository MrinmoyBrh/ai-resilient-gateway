import OpenAI from 'openai';
import { GoogleGenAI } from '@google/genai';
import { AIJobPayload, AIJobResult, IncidentTriageOutput } from '../types/index.js';
import dotenv from 'dotenv';

dotenv.config();

// Free Provider 1: Groq (via OpenAI SDK)
const groq = new OpenAI({
  apiKey: process.env.GROQ_API_KEY || '',
  baseURL: 'https://api.groq.com/openai/v1',
});

// Free Provider 2: Google Gemini (Official Gen AI SDK)
const gemini = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY || '' });

const SYSTEM_PROMPT = `You are a site reliability and triage engineer.
Analyze the provided log/incident report and return ONLY valid JSON matching this schema:
{
  "severity": "LOW" | "MEDIUM" | "HIGH" | "CRITICAL",
  "rootCause": "concise description",
  "affectedService": "service name",
  "recommendedAction": "clear remediation step"
}`;

export class LLMOrchestrator {
  public static async executeWithFallback(payload: AIJobPayload): Promise<AIJobResult> {
    const startTime = Date.now();

    try {
      // 1. Primary: Groq (Llama-3.3-70b-versatile or Llama-3.1-8b-instant)
      return await this.callGroq(payload, startTime);
    } catch (primaryError: any) {
      console.warn(`[Orchestrator] Primary (Groq) failed: ${primaryError.message}. Switching to Gemini fallback...`);

      try {
        // 2. Secondary Fallback: Google Gemini
        return await this.callGemini(payload, startTime);
      } catch (fallbackError: any) {
        throw new Error(`All free providers failed. Groq: ${primaryError.message} | Gemini: ${fallbackError.message}`);
      }
    }
  }

 // Inside src/workers/orchestrator.ts

// 1. In callGroq(): Change model to 'llama-3.3-70b-versatile'
private static async callGroq(payload: AIJobPayload, startTime: number): Promise<AIJobResult> {
    const response = await groq.chat.completions.create({
      model: 'gpt-oss-20b', // <--- ACTIVE GROQ MODEL
      messages: [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content: payload.prompt },
      ],
      response_format: { type: 'json_object' },
      temperature: 0.1,
    });

    const rawContent = response.choices[0]?.message?.content || '{}';
    const parsed: IncidentTriageOutput = JSON.parse(rawContent);

    return {
      jobId: payload.jobId,
      content: parsed,
      providerResolved: 'groq',
      modelResolved: 'gpt-oss-20b',
      latencyMs: Date.now() - startTime,
      tokens: {
        prompt: response.usage?.prompt_tokens ?? 0,
        completion: response.usage?.completion_tokens ?? 0,
      },
    };
  }

  private static async callGemini(payload: AIJobPayload, startTime: number): Promise<AIJobResult> {
    const response = await gemini.models.generateContent({
      model: 'gemini-3.8-flash', // <--- ACTIVE GEMINI MODEL
      contents: `${SYSTEM_PROMPT}\n\nIncident to analyze:\n${payload.prompt}`,
      config: {
        responseMimeType: 'application/json',
        temperature: 0.1,
      },
    });

    const rawContent = response.text || '{}';
    const parsed: IncidentTriageOutput = JSON.parse(rawContent);

    return {
      jobId: payload.jobId,
      content: parsed,
      providerResolved: 'gemini',
      modelResolved: 'gemini-3.8-flash',
      latencyMs: Date.now() - startTime,
      tokens: {
        prompt: Math.ceil(payload.prompt.length / 4),
        completion: Math.ceil(rawContent.length / 4),
      },
    };
  }
}


import { CircuitBreaker } from './circuitBreaker.js';
import { AIJobPayload } from '../types/index.js';

const groqBreaker = new CircuitBreaker('groq', { failureThreshold: 3, cooldownPeriodMs: 20000 });
const geminiBreaker = new CircuitBreaker('gemini', { failureThreshold: 3, cooldownPeriodMs: 20000 });

export class LLMOrchestrator {
  static async executeWithFallback(payload: AIJobPayload) {
    const start = Date.now();
    const prompt = payload.prompt;

    // 1. Check Primary Provider (Groq)
    const groqState = await groqBreaker.getState();

    if (groqState !== 'OPEN') {
      try {
        console.log(`[Orchestrator] Attempting Groq (Circuit: ${groqState})...`);
        const result = await this.callGroq(prompt);
        await groqBreaker.recordSuccess();

        return {
          content: result,
          providerResolved: 'groq',
          modelResolved: 'llama-3.3-70b-versatile',
          latencyMs: Date.now() - start,
          tokens: { prompt: 15, completion: 45 },
        };
      } catch (err: any) {
        console.warn(`[Orchestrator] Groq invocation failed: ${err.message}`);
        await groqBreaker.recordFailure();
      }
    } else {
      console.warn(`[Orchestrator] Groq circuit is OPEN. Fast-failing over directly to Gemini.`);
    }

    // 2. Fallback Provider (Gemini)
    const geminiState = await geminiBreaker.getState();
    if (geminiState !== 'OPEN') {
      try {
        console.log(`[Orchestrator] Attempting Gemini fallback (Circuit: ${geminiState})...`);
        const result = await this.callGemini(prompt);
        await geminiBreaker.recordSuccess();

        return {
          content: result,
          providerResolved: 'gemini',
          modelResolved: 'gemini-1.5-flash',
          latencyMs: Date.now() - start,
          tokens: { prompt: 15, completion: 45 },
        };
      } catch (err: any) {
        console.error(`[Orchestrator] Gemini invocation failed: ${err.message}`);
        await geminiBreaker.recordFailure();
        throw err;
      }
    }

    throw new Error('All configured AI providers are degraded (Circuits OPEN).');
  }

  private static async callGroq(prompt: string) {
    // Uses your existing Groq SDK / fetch call
    return { triage: `Resolved via Primary: ${prompt}` };

    // Temporary: simulate provider outage / 503 Service Unavailable
  // throw new Error('503 Upstream Groq Service Unavailable');
  }

  private static async callGemini(prompt: string) {
    // Uses your existing Gemini SDK / fetch call
    throw new Error('Simulated upstream network timeout');
  }

  // src/workers/orchestrator.ts


}
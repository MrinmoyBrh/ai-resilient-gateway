import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { TelemetryService } from '../../services/telemetry.js';

export async function analyticsRoutes(server: FastifyInstance) {
  server.get('/v1/analytics/tenant', async (request: FastifyRequest, reply: FastifyReply) => {
    try {
      const apiKey = request.headers['x-api-key'] as string;
      const tenantId = (request as any).tenant?.id || 'a0000000-0000-0000-0000-000000000001';

      const analytics = await TelemetryService.getTenantAnalytics(tenantId);
      return reply.status(200).send(analytics);
    } catch (err: any) {
      request.log.error(err);
      return reply.status(500).send({
        statusCode: 500,
        error: 'Internal Server Error',
        message: err.message,
      });
    }
  });
}
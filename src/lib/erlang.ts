/**
 * Erlang C calculations for contact center staffing
 */

export interface ErlangResult {
  agents: number;
  serviceLevel: number;
  occupancy: number;
  waitingProbability: number;
  averageSpeedOfAnswer: number;
}

/**
 * Calculates Erlang C probability
 * @param intensity Traffic intensity (A = calls_per_hour * AHT / 3600)
 * @param agents Number of agents (m)
 */
function calculateErlangC(intensity: number, agents: number): number {
  if (agents <= intensity) return 1;

  let sum = 0;
  for (let i = 0; i < agents; i++) {
    sum += Math.pow(intensity, i) / factorial(i);
  }

  const term = Math.pow(intensity, agents) / (factorial(agents) * (1 - intensity / agents));
  return term / (sum + term);
}

function factorial(n: number): number {
  if (n === 0) return 1;
  let res = 1;
  for (let i = 2; i <= n; i++) res *= i;
  return res;
}

/**
 * Finds the minimum number of agents to meet a service level target
 * @param callsPerHour Number of calls in the hour
 * @param aht Average Handling Time in seconds
 * @param targetSL Service Level target (e.g., 0.8 for 80%)
 * @param targetTime Target time in seconds (e.g., 20)
 */
export function calculateRequiredAgents(
  callsPerHour: number,
  aht: number,
  targetSL: number,
  targetTime: number
): ErlangResult {
  const intensity = (callsPerHour * aht) / 3600;
  let agents = Math.ceil(intensity) + 1;

  while (true) {
    const pw = calculateErlangC(intensity, agents);
    const sl = 1 - pw * Math.exp(-(agents - intensity) * (targetTime / aht));
    const occupancy = intensity / agents;

    if (sl >= targetSL || agents > 500) { // Cap at 500 to prevent infinite loops
      return {
        agents,
        serviceLevel: sl,
        occupancy,
        waitingProbability: pw,
        averageSpeedOfAnswer: (pw * aht) / (agents - intensity)
      };
    }
    agents++;
  }
}

/**
 * Parses the CSV data provided by the user
 * Format: Time Interval;Day1;Day2...
 */
export function parseVolumeCSV(csv: string): { [day: string]: { [interval: string]: number } } {
  const lines = csv.trim().split('\n');
  if (lines.length < 4) return {};

  // Find the header with day numbers (usually line 2 or 3)
  // Based on user input:
  // Line 0: Total Volume...
  // Line 1: ;01;02;03...
  const dayHeaders = lines[1].split(';').filter(s => s.trim() !== '');
  const result: { [day: string]: { [interval: string]: number } } = {};

  dayHeaders.forEach(day => {
    result[day] = {};
  });

  // Data starts from line 4 (0:00 - 1:00...)
  for (let i = 4; i < lines.length; i++) {
    const parts = lines[i].split(';');
    const interval = parts[0].trim();
    if (!interval) continue;

    for (let j = 1; j < parts.length; j++) {
      const dayIndex = j - 1;
      if (dayIndex < dayHeaders.length) {
        const day = dayHeaders[dayIndex];
        const volume = parseInt(parts[j]) || 0;
        result[day][interval] = volume;
      }
    }
  }

  return result;
}

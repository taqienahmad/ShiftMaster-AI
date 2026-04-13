import { GoogleGenAI, Type } from "@google/genai";

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY || "" });

export interface ForecastData {
  date: string;
  predictedDemand: number;
  requiredStaffCount: number;
}

export async function generateStaffingForecast(historicalData: any[]): Promise<ForecastData[]> {
  const prompt = `
    Based on the following historical staffing and demand data, predict the staffing requirements for the next 7 days.
    Historical Data: ${JSON.stringify(historicalData)}
    
    Provide a forecast for each of the next 7 days starting from today.
    Return the result as a JSON array of objects with 'date' (YYYY-MM-DD), 'predictedDemand' (number), and 'requiredStaffCount' (integer).
  `;

  try {
    const response = await ai.models.generateContent({
      model: "gemini-3-flash-preview",
      contents: prompt,
      config: {
        responseMimeType: "application/json",
        responseSchema: {
          type: Type.ARRAY,
          items: {
            type: Type.OBJECT,
            properties: {
              date: { type: Type.STRING },
              predictedDemand: { type: Type.NUMBER },
              requiredStaffCount: { type: Type.INTEGER }
            },
            required: ["date", "predictedDemand", "requiredStaffCount"]
          }
        }
      }
    });

    return JSON.parse(response.text || "[]");
  } catch (error) {
    console.error("Error generating forecast:", error);
    return [];
  }
}

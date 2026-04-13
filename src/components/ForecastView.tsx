import React, { useState, useEffect } from 'react';
import { db, collection, onSnapshot, query, setDoc, doc, Timestamp, OperationType, handleFirestoreError } from '../lib/firebase';
import { generateStaffingForecast, ForecastData } from '../lib/gemini';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  BarChart,
  Bar,
  Legend
} from 'recharts';
import { Brain, Sparkles, RefreshCw, AlertCircle, Calculator } from 'lucide-react';
import { toast } from 'sonner';
import { format } from 'date-fns';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import ErlangForecaster from './ErlangForecaster';
import { ShiftCode } from './ShiftCodeManager';

interface Employee {
  id: string;
  nip: string;
  name: string;
  email: string;
  skill: string;
  channel: string;
  site: string;
  gender: string;
  religion: string;
  role: string;
  department: string;
  skills: string[];
}

interface ForecastVolumeData {
  date: string;
  day: string;
  totalVolume: number;
  intervals: Record<string, number>;
}

export default function ForecastView({ isAdmin }: { isAdmin: boolean }) {
  const [forecasts, setForecasts] = useState<ForecastData[]>([]);
  const [loading, setLoading] = useState(false);
  const [volumeData, setVolumeData] = useState<ForecastVolumeData[]>([]);
  const [shiftCodes, setShiftCodes] = useState<ShiftCode[]>([]);
  const [employees, setEmployees] = useState<Employee[]>([]);

  useEffect(() => {
    // Listen to AI Forecasts
    const qForecasts = query(collection(db, 'forecasts'));
    const unsubForecasts = onSnapshot(qForecasts, (snapshot) => {
      const list = snapshot.docs.map(doc => {
        const data = doc.data();
        return {
          ...data,
          date: data.date instanceof Timestamp ? format(data.date.toDate(), 'yyyy-MM-dd') : data.date
        } as ForecastData;
      });
      setForecasts(list.sort((a, b) => a.date.localeCompare(b.date)));
    });

    // Listen to Forecast Volume (The data uploaded in the previous step)
    const qVolume = query(collection(db, 'forecastVolume'));
    const unsubVolume = onSnapshot(qVolume, (snapshot) => {
      const list = snapshot.docs.map(doc => doc.data() as ForecastVolumeData);
      setVolumeData(list.sort((a, b) => a.date.localeCompare(b.date)));
    }, (error) => {
      handleFirestoreError(error, OperationType.LIST, 'forecastVolume');
    });

    // Listen to Shift Codes
    const qShifts = query(collection(db, 'shiftCodes'));
    const unsubShifts = onSnapshot(qShifts, (snapshot) => {
      const list = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as ShiftCode));
      setShiftCodes(list);
    });

    // Listen to Employees
    const qEmployees = query(collection(db, 'employees'));
    const unsubEmployees = onSnapshot(qEmployees, (snapshot) => {
      const list = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Employee));
      setEmployees(list);
    });

    return () => {
      unsubForecasts();
      unsubVolume();
      unsubShifts();
      unsubEmployees();
    };
  }, []);

  const handleGenerateForecast = async () => {
    if (!isAdmin) return;
    setLoading(true);
    try {
      // Use volumeData as historical context for the AI
      const context = volumeData.map(v => ({
        date: v.date,
        totalVolume: v.totalVolume
      }));
      
      const result = await generateStaffingForecast(context);
      if (result && result.length > 0) {
        // Save to Firestore
        for (const item of result) {
          const id = `forecast_${item.date}`;
          await setDoc(doc(db, 'forecasts', id), {
            ...item,
            date: Timestamp.fromDate(new Date(item.date))
          });
        }
        toast.success('AI Forecast generated successfully');
      } else {
        toast.error('Failed to generate forecast');
      }
    } catch (error) {
      console.error(error);
      toast.error('Error in AI processing');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <Tabs defaultValue="erlang" className="w-full">
        <div className="flex items-center justify-between mb-6">
          <TabsList className="bg-white border border-slate-200">
            <TabsTrigger value="erlang" className="gap-2">
              <Calculator className="w-4 h-4" />
              Erlang C Forecast
            </TabsTrigger>
            <TabsTrigger value="ai" className="gap-2">
              <Sparkles className="w-4 h-4" />
              AI Insights
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="erlang" className="mt-0">
          <div className="mb-6">
            <h1 className="text-3xl font-bold tracking-tight text-slate-900">Staffing Requirement</h1>
            <p className="text-slate-500">Calculate headcount needed based on your uploaded volume and shift patterns.</p>
          </div>
          <ErlangForecaster volumeData={volumeData} shiftCodes={shiftCodes} employees={employees} />
        </TabsContent>

        <TabsContent value="ai" className="space-y-6 mt-0">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div>
              <h1 className="text-3xl font-bold tracking-tight text-slate-900">AI Staffing Forecast</h1>
              <p className="text-slate-500">Predictive analytics for optimal shift planning.</p>
            </div>

            {isAdmin && (
              <Button 
                onClick={handleGenerateForecast} 
                disabled={loading}
                className="gap-2 bg-gradient-to-r from-indigo-600 to-violet-600 hover:from-indigo-700 hover:to-violet-700 text-white border-none shadow-lg shadow-indigo-200"
              >
                {loading ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
                Generate AI Forecast
              </Button>
            )}
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <Card className="lg:col-span-2 border-none shadow-sm h-[400px]">
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  <TrendingUp className="w-5 h-5 text-indigo-500" />
                  Predicted Demand & Staffing
                </CardTitle>
                <CardDescription>7-day outlook based on historical patterns.</CardDescription>
              </CardHeader>
              <CardContent className="h-[300px]">
                {forecasts.length > 0 ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={forecasts}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                      <XAxis dataKey="date" axisLine={false} tickLine={false} tick={{fontSize: 12, fill: '#64748b'}} />
                      <YAxis axisLine={false} tickLine={false} tick={{fontSize: 12, fill: '#64748b'}} />
                      <Tooltip 
                        contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                      />
                      <Legend iconType="circle" />
                      <Bar dataKey="predictedDemand" name="Predicted Demand" fill="#818cf8" radius={[4, 4, 0, 0]} />
                      <Bar dataKey="requiredStaffCount" name="Required Staff" fill="#4f46e5" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-full flex flex-col items-center justify-center text-slate-400 space-y-4">
                    <Brain className="w-12 h-12 opacity-20" />
                    <p>No forecast data available. Click generate to start.</p>
                  </div>
                )}
              </CardContent>
            </Card>

            <Card className="border-none shadow-sm">
              <CardHeader>
                <CardTitle className="text-lg">AI Insights</CardTitle>
                <CardDescription>Smart recommendations for your team.</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                {forecasts.length > 0 ? (
                  <>
                    <div className="p-4 bg-indigo-50 rounded-xl border border-indigo-100">
                      <h4 className="font-bold text-indigo-900 text-sm flex items-center gap-2">
                        <Sparkles className="w-4 h-4" />
                        Peak Demand Alert
                      </h4>
                      <p className="text-xs text-indigo-700 mt-1">
                        High demand expected on {forecasts.reduce((prev, current) => (prev.predictedDemand > current.predictedDemand) ? prev : current).date}. Consider adding extra morning shifts.
                      </p>
                    </div>
                    <div className="p-4 bg-emerald-50 rounded-xl border border-emerald-100">
                      <h4 className="font-bold text-emerald-900 text-sm flex items-center gap-2">
                        <AlertCircle className="w-4 h-4" />
                        Efficiency Tip
                      </h4>
                      <p className="text-xs text-emerald-700 mt-1">
                        Staffing levels are optimal for the upcoming weekend. No adjustments needed.
                      </p>
                    </div>
                  </>
                ) : (
                  <div className="text-sm text-slate-500 italic">
                    Generate a forecast to see AI insights.
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>

      </Tabs>
    </div>
  );
}

function TrendingUp(props: any) {
  return (
    <svg
      {...props}
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <polyline points="22 7 13.5 15.5 8.5 10.5 2 17" />
      <polyline points="16 7 22 7 22 13" />
    </svg>
  )
}

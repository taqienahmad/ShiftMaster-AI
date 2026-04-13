import React, { useState, useEffect } from 'react';
import { db, collection, onSnapshot, query, setDoc, doc, OperationType, handleFirestoreError, writeBatch } from '../lib/firebase';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { calculateRequiredAgents, ErlangResult } from '@/src/lib/erlang';
import { Calculator, FileText, TrendingUp, Info, Clock, Users, CheckCircle2, Calendar, RefreshCw, AlertCircle } from 'lucide-react';
import { toast } from 'sonner';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, BarChart, Bar } from 'recharts';
import { ShiftCode } from './ShiftCodeManager';

interface ForecastVolumeData {
  date: string;
  day: string;
  totalVolume: number;
  intervals: Record<string, number>;
}

interface Employee {
  id: string;
  nip: string;
  name: string;
  skill: string;
}

export default function ErlangForecaster({ volumeData, shiftCodes, employees }: { volumeData: ForecastVolumeData[], shiftCodes: ShiftCode[], employees: Employee[] }) {
  const [aht, setAht] = useState(300); // Default 300s
  const [targetSL, setTargetSL] = useState(80); // Default 80%
  const [targetTime, setTargetTime] = useState(20); // Default 20s
  const [shrinkage, setShrinkage] = useState(30); // Default 30%
  const [results, setResults] = useState<{ 
    date: string, 
    day: string, 
    totalAgents: number, 
    peakAgents: number,
    intervalNeeds: Record<string, number>,
    shiftSuggestions: Record<string, number>,
    assignments: Record<string, string>, // employeeId -> shiftCode
    intervalSupply: Record<string, number> // interval -> actual agents on duty
  }[]>([]);
  const [calculating, setCalculating] = useState(false);
  const [lastAppliedSettings, setLastAppliedSettings] = useState<any>(null);

  // Load settings on mount
  useEffect(() => {
    const unsub = onSnapshot(doc(db, 'erlangSettings', 'current'), (snapshot) => {
      if (snapshot.exists()) {
        const data = snapshot.data();
        if (data.aht) setAht(data.aht);
        if (data.targetSL) setTargetSL(data.targetSL);
        if (data.targetTime) setTargetTime(data.targetTime);
        if (data.shrinkage) setShrinkage(data.shrinkage);
        if (data.lastApplied) setLastAppliedSettings(data.lastApplied);
      }
    });
    return () => unsub();
  }, []);

  const isStale = lastAppliedSettings && (
    aht !== lastAppliedSettings.aht ||
    targetSL !== lastAppliedSettings.targetSL ||
    targetTime !== lastAppliedSettings.targetTime ||
    shrinkage !== lastAppliedSettings.targetShrinkage
  );

  // Save settings when they change
  const saveSettings = async (newSettings: any) => {
    try {
      await setDoc(doc(db, 'erlangSettings', 'current'), newSettings, { merge: true });
    } catch (error) {
      console.error('Error saving settings:', error);
    }
  };

  const handleReset = async () => {
    const defaults = {
      aht: 300,
      targetSL: 80,
      targetTime: 20,
      shrinkage: 30
    };
    setAht(defaults.aht);
    setTargetSL(defaults.targetSL);
    setTargetTime(defaults.targetTime);
    setShrinkage(defaults.shrinkage);
    await saveSettings(defaults);
    toast.success('Parameters reset to defaults');
  };

  useEffect(() => {
    const q = query(collection(db, 'erlangResults'));
    const unsubscribe = onSnapshot(q, (snapshot) => {
      const list = snapshot.docs.map(doc => doc.data() as any);
      if (list.length > 0) {
        setResults(list.sort((a, b) => a.date.localeCompare(b.date)));
      }
    }, (error) => {
      handleFirestoreError(error, OperationType.LIST, 'erlangResults');
    });
    return () => unsubscribe();
  }, []);

  const handleCalculate = async () => {
    if (volumeData.length === 0) {
      toast.error('No volume data found. Please upload volume data first.');
      return;
    }

    setCalculating(true);
    try {
      const dailyResults: any[] = [];
      const batch = writeBatch(db);

      volumeData.forEach((data) => {
        let dayPeak = 0;
        let dayTotalHours = 0;
        const intervalNeeds: Record<string, number> = {};

        Object.entries(data.intervals).forEach(([interval, volume]) => {
          const res = calculateRequiredAgents(volume, aht, targetSL / 100, targetTime);
          intervalNeeds[interval] = res.agents;
          dayPeak = Math.max(dayPeak, res.agents);
          dayTotalHours += res.agents;
        });

        // Simple headcount calculation (Total hours / 8 hours per shift * shrinkage)
        const headcount = Math.ceil((dayTotalHours / 8) * (1 + shrinkage / 100));

        // Improved shift suggestion logic:
        const shiftSuggestions: Record<string, number> = {};
        if (shiftCodes.length > 0) {
          const shiftVolumes: Record<string, number> = {};
          let totalShiftVolume = 0;
          let minCoverageAssigned = 0;

          // Calculate volumes per shift
          shiftCodes.forEach(code => {
            let coveredVolume = 0;
            Object.entries(data.intervals).forEach(([interval, volume]) => {
              if (isIntervalInShift(interval, code.startTime, code.endTime)) {
                coveredVolume += volume;
              }
            });
            shiftVolumes[code.code] = coveredVolume;
            totalShiftVolume += coveredVolume;
            shiftSuggestions[code.code] = 0;
          });

          // Step 1: Minimum coverage for shifts with demand
          const shiftsWithDemand = shiftCodes.filter(c => shiftVolumes[c.code] > 0);
          if (headcount >= shiftsWithDemand.length) {
            shiftsWithDemand.forEach(code => {
              shiftSuggestions[code.code] = 1;
              minCoverageAssigned++;
            });
          }

          // Step 2: Distribute remaining headcount proportionally
          const remainingHeadcount = headcount - minCoverageAssigned;
          if (remainingHeadcount > 0 && totalShiftVolume > 0) {
            shiftCodes.forEach(code => {
              const proportion = shiftVolumes[code.code] / totalShiftVolume;
              const additional = Math.floor(remainingHeadcount * proportion);
              shiftSuggestions[code.code] += additional;
            });

            let currentTotal = Object.values(shiftSuggestions).reduce((a, b) => a + b, 0);
            let leftover = headcount - currentTotal;
            
            const sortedShifts = [...shiftCodes].sort((a, b) => shiftVolumes[b.code] - shiftVolumes[a.code]);
            for (let i = 0; i < leftover && i < sortedShifts.length; i++) {
              shiftSuggestions[sortedShifts[i].code]++;
            }
          } else if (remainingHeadcount > 0) {
            shiftCodes.forEach((code, i) => {
              const extra = i < remainingHeadcount ? 1 : 0;
              shiftSuggestions[code.code] += extra;
            });
          }
        }

        // Generate Draft Assignments
        const assignments: Record<string, string> = {};
        if (employees.length > 0 && shiftCodes.length > 0) {
          let empIndex = 0;
          Object.entries(shiftSuggestions).forEach(([code, count]) => {
            for (let i = 0; i < count; i++) {
              if (empIndex < employees.length) {
                assignments[employees[empIndex].id] = code;
                empIndex++;
              }
            }
          });
          for (let i = empIndex; i < employees.length; i++) {
            assignments[employees[i].id] = 'OFF';
          }
        }

        // Calculate Actual Interval Supply based on assignments
        const intervalSupply: Record<string, number> = {};
        Object.keys(data.intervals).forEach(interval => {
          let supply = 0;
          Object.values(assignments).forEach(shiftCode => {
            if (shiftCode === 'OFF') return;
            const codeObj = shiftCodes.find(c => c.code === shiftCode);
            if (codeObj && isIntervalInShift(interval, codeObj.startTime, codeObj.endTime)) {
              supply++;
            }
          });
          intervalSupply[interval] = supply;
        });

        const resultItem = {
          date: data.date,
          day: data.day,
          totalAgents: headcount,
          peakAgents: dayPeak,
          intervalNeeds,
          shiftSuggestions,
          assignments,
          intervalSupply
        };

        dailyResults.push(resultItem);
        const docRef = doc(db, 'erlangResults', data.date);
        batch.set(docRef, resultItem);
      });

      await batch.commit();
      
      // Save these as last applied
      const applied = { aht, targetSL, targetTime, targetShrinkage: shrinkage };
      await setDoc(doc(db, 'erlangSettings', 'current'), { lastApplied: applied }, { merge: true });
      setLastAppliedSettings(applied);
      
      setResults(dailyResults);
      toast.success('Staffing requirements calculated and saved');
    } catch (error) {
      console.error(error);
      toast.error('Error calculating requirements');
    } finally {
      setCalculating(false);
    }
  };

  // Helper to check if an interval falls within a shift
  function isIntervalInShift(interval: string, shiftStart: string, shiftEnd: string): boolean {
    try {
      const [intStartStr] = interval.split(' - ');
      const [intH] = intStartStr.split(':').map(Number);
      const [startH] = shiftStart.split(':').map(Number);
      const [endH] = shiftEnd.split(':').map(Number);

      if (startH < endH) {
        return intH >= startH && intH < endH;
      } else {
        // Overnight shift (e.g., 22:00 - 06:00)
        return intH >= startH || intH < endH;
      }
    } catch (e) {
      return false;
    }
  }

  return (
    <div className="space-y-6">
      <Card className="border-none shadow-sm bg-indigo-50/50 border border-indigo-100">
        <CardContent className="p-6">
          <div className="flex gap-4">
            <div className="w-10 h-10 rounded-full bg-indigo-100 flex items-center justify-center shrink-0">
              <Info className="w-5 h-5 text-indigo-600" />
            </div>
            <div className="space-y-2">
              <h3 className="font-bold text-indigo-900">Staffing Calculation</h3>
              <p className="text-sm text-indigo-700 leading-relaxed">
                This tool uses the <strong>Erlang C</strong> formula to calculate the number of agents needed for each interval based on your uploaded volume. 
                It then suggests a headcount and shift distribution to meet your service level targets.
              </p>
              <div className="flex items-center gap-4 pt-2">
                <div className="flex items-center gap-2 text-xs font-medium text-indigo-600">
                  <CheckCircle2 className="w-4 h-4" />
                  {volumeData.length} Days of Volume Data
                </div>
                <div className="flex items-center gap-2 text-xs font-medium text-indigo-600">
                  <CheckCircle2 className="w-4 h-4" />
                  {shiftCodes.length} Active Shift Codes
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        <Card className="lg:col-span-1 border-none shadow-sm h-fit">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Calculator className="w-5 h-5 text-primary" />
              Parameters
            </CardTitle>
            <CardDescription>Configure Erlang C variables</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Avg Handling Time (sec)</label>
              <Input type="number" value={aht} onChange={e => {
                const val = Number(e.target.value);
                setAht(val);
                saveSettings({ aht: val });
              }} />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Service Level Target (%)</label>
              <Input type="number" value={targetSL} onChange={e => {
                const val = Number(e.target.value);
                setTargetSL(val);
                saveSettings({ targetSL: val });
              }} />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Target Time (sec)</label>
              <Input type="number" value={targetTime} onChange={e => {
                const val = Number(e.target.value);
                setTargetTime(val);
                saveSettings({ targetTime: val });
              }} />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Shrinkage (%)</label>
              <Input type="number" value={shrinkage} onChange={e => {
                const val = Number(e.target.value);
                setShrinkage(val);
                saveSettings({ shrinkage: val });
              }} />
            </div>
            <div className="pt-2 flex flex-col gap-2">
              <Button 
                className="w-full h-12 text-lg font-bold shadow-lg shadow-primary/20" 
                onClick={handleCalculate}
                disabled={volumeData.length === 0 || calculating}
              >
                {calculating ? <RefreshCw className="w-5 h-5 animate-spin mr-2" /> : null}
                Calculate Requirements
              </Button>
              <Button 
                variant="ghost" 
                size="sm" 
                className="text-slate-400 hover:text-slate-600"
                onClick={handleReset}
              >
                Reset to Defaults
              </Button>
            </div>
          </CardContent>
        </Card>

        <div className="lg:col-span-3 space-y-6">
          {results.length > 0 ? (
            <>
              <Card className="border-none shadow-sm h-[400px]">
                <CardHeader className="flex flex-row items-center justify-between">
                  <div>
                    <CardTitle className="flex items-center gap-2">
                      <TrendingUp className="w-5 h-5 text-primary" />
                      Staffing Forecast Chart
                    </CardTitle>
                    <CardDescription>Daily headcount and peak concurrency needs.</CardDescription>
                  </div>
                  <div className="flex items-center gap-3">
                    {isStale && (
                      <Badge variant="destructive" className="animate-pulse gap-1 py-1">
                        <AlertCircle className="w-3 h-3" />
                        Settings Changed
                      </Badge>
                    )}
                    <div className="text-xs text-slate-400 font-medium hidden md:block">
                      {isStale ? 'Recalculate to update roster' : 'Ready for scheduling?'}
                    </div>
                    <Button 
                      variant={isStale ? "default" : "outline"} 
                      size="sm" 
                      className={`gap-2 ${isStale ? 'bg-amber-500 hover:bg-amber-600 text-white border-none' : 'text-primary border-primary/20 hover:bg-primary/5'}`} 
                      onClick={() => window.location.hash = '#roster'}
                    >
                      <Calendar className="w-4 h-4" />
                      Go to Roster
                    </Button>
                  </div>
                </CardHeader>
                <CardContent className="h-[300px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={results}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                      <XAxis dataKey="date" axisLine={false} tickLine={false} tick={{fontSize: 12, fill: '#64748b'}} />
                      <YAxis axisLine={false} tickLine={false} tick={{fontSize: 12, fill: '#64748b'}} />
                      <Tooltip 
                        contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                      />
                      <Legend iconType="circle" />
                      <Line type="monotone" dataKey="totalAgents" name="Total Headcount" stroke="#4f46e5" strokeWidth={3} dot={{ r: 4, fill: '#4f46e5' }} activeDot={{ r: 6 }} />
                      <Line type="monotone" dataKey="peakAgents" name="Peak Concurrency" stroke="#f59e0b" strokeWidth={3} dot={{ r: 4, fill: '#f59e0b' }} activeDot={{ r: 6 }} />
                    </LineChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>

              <Card className="border-none shadow-sm overflow-hidden">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Clock className="w-5 h-5 text-primary" />
                    Shift Distribution Summary
                  </CardTitle>
                  <CardDescription>Visibility of agents needed per shift across all dates.</CardDescription>
                </CardHeader>
                <CardContent className="p-0">
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader className="bg-slate-50">
                        <TableRow>
                          <TableHead className="min-w-[100px] sticky left-0 bg-slate-50 z-20">Shift</TableHead>
                          <TableHead className="min-w-[120px] sticky left-[100px] bg-slate-50 z-20 border-r">Interval</TableHead>
                          {results.map(res => (
                            <TableHead key={res.date} className="min-w-[100px] text-center">
                              <div className="flex flex-col items-center">
                                <span className="text-xs font-bold">{res.date}</span>
                                <span className="text-[10px] text-slate-500 uppercase">{res.day.substring(0, 3)}</span>
                              </div>
                            </TableHead>
                          ))}
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {shiftCodes.length > 0 ? (
                          shiftCodes.map(code => (
                            <TableRow key={code.code} className="hover:bg-slate-50/50 transition-colors">
                              <TableCell className="font-bold text-slate-900 sticky left-0 bg-white z-10">{code.code}</TableCell>
                              <TableCell className="text-xs text-slate-500 sticky left-[100px] bg-white z-10 border-r">{code.startTime} - {code.endTime}</TableCell>
                              {results.map(res => (
                                <TableCell key={res.date} className="text-center">
                                  <span className={`font-medium ${res.shiftSuggestions[code.code] > 0 ? 'text-indigo-600 font-bold' : 'text-slate-300'}`}>
                                    {res.shiftSuggestions[code.code] || 0}
                                  </span>
                                </TableCell>
                              ))}
                            </TableRow>
                          ))
                        ) : (
                          <TableRow>
                            <TableCell colSpan={results.length + 2} className="h-24 text-center text-slate-500 italic">
                              No shift codes defined to suggest coverage.
                            </TableCell>
                          </TableRow>
                        )}
                        {/* Total Row */}
                        <TableRow className="bg-slate-50/50 font-bold">
                          <TableCell className="sticky left-0 bg-slate-50 z-10">Total Need</TableCell>
                          <TableCell className="sticky left-[100px] bg-slate-50 z-10 border-r"></TableCell>
                          {results.map(res => (
                            <TableCell key={res.date} className="text-center text-primary">
                              {res.totalAgents}
                            </TableCell>
                          ))}
                        </TableRow>
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>

              <Card className="border-none shadow-sm overflow-hidden">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Clock className="w-5 h-5 text-primary" />
                    Interval Coverage Analysis (Supply vs Demand)
                  </CardTitle>
                  <CardDescription>
                    Visibility of concurrent agents on duty per 1-hour interval. 
                    <span className="text-red-500 font-bold ml-2">Red</span> indicates understaffed gaps.
                  </CardDescription>
                </CardHeader>
                <CardContent className="p-0">
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader className="bg-slate-50">
                        <TableRow>
                          <TableHead className="min-w-[150px] sticky left-0 bg-slate-50 z-20 border-r">Interval</TableHead>
                          {results.map(res => (
                            <TableHead key={res.date} className="min-w-[120px] text-center">
                              <div className="flex flex-col items-center">
                                <span className="text-xs font-bold">{res.date}</span>
                                <span className="text-[10px] text-slate-500 uppercase">{res.day.substring(0, 3)}</span>
                              </div>
                            </TableHead>
                          ))}
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {Object.keys(results[0].intervalNeeds)
                          .sort((a, b) => {
                            const hourA = parseInt(a.split(':')[0]);
                            const hourB = parseInt(b.split(':')[0]);
                            return hourA - hourB;
                          })
                          .map(interval => (
                            <TableRow key={interval} className="hover:bg-slate-50/50 transition-colors">
                            <TableHead className="font-medium text-slate-700 sticky left-0 bg-white z-10 border-r text-xs">
                              {interval}
                            </TableHead>
                            {results.map(res => {
                              const supply = res.intervalSupply[interval] || 0;
                              const demand = res.intervalNeeds[interval] || 0;
                              const isUnderstaffed = supply < demand;
                              const isNoStaff = supply === 0 && demand > 0;
                              
                              return (
                                <TableCell key={res.date} className="text-center p-2">
                                  <div className={`flex flex-col items-center justify-center rounded-lg p-1 border ${
                                    isNoStaff 
                                      ? 'bg-red-100 border-red-300 text-red-700 animate-pulse' 
                                      : isUnderstaffed 
                                        ? 'bg-orange-50 border-orange-200 text-orange-700' 
                                        : 'bg-emerald-50 border-emerald-100 text-emerald-700'
                                  }`}>
                                    <span className="text-sm font-bold">{supply}</span>
                                    <div className="flex items-center gap-1 text-[10px] opacity-70">
                                      <span>Req:</span>
                                      <span>{demand}</span>
                                    </div>
                                  </div>
                                </TableCell>
                              );
                            })}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            </>
          ) : (
            <div className="h-[400px] flex flex-col items-center justify-center bg-white rounded-xl border border-dashed border-slate-200 text-slate-400 space-y-4">
              <Calculator className="w-12 h-12 opacity-20" />
              <div className="text-center">
                <p className="font-medium">Ready to calculate</p>
                <p className="text-sm">Adjust parameters and click calculate to see requirements.</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}


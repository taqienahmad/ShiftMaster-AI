import React, { useState, useEffect } from 'react';
import { db, collection, onSnapshot, query, setDoc, doc, OperationType, handleFirestoreError } from '../lib/firebase';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Calendar as CalendarIcon, Users, Download, RefreshCw, CheckCircle2, AlertCircle, Clock, Sparkles, TrendingUp, Activity, Target, ShieldCheck, Zap, Heart, BarChart3, ClipboardCheck } from 'lucide-react';
import { toast } from 'sonner';
import { format, startOfWeek, addDays, isSameDay } from 'date-fns';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { ShiftCode } from './ShiftCodeManager';
import { calculateRequiredAgents } from '../lib/erlang';

interface Employee {
  id: string;
  nip: string;
  name: string;
  skill: string;
}

interface ForecastVolumeData {
  date: string;
  day: string;
  totalVolume: number;
  intervals: Record<string, number>;
}

interface RosterDay {
  date: string;
  shiftCode: string;
}

interface EmployeeRoster {
  employeeId: string;
  employeeName: string;
  nip: string;
  days: Record<string, string>; // date -> shiftCode
  totalWorkingDays: number;
  totalOffDays: number;
  targetWorkingDays: number;
  targetOffDays: number;
}

interface WorkingDayRef {
  id: string;
  month: string;
  totalDays: number;
  workingDays: number;
  weekend: number;
  holiday: number;
}

export default function RosterView({ isAdmin }: { isAdmin: boolean }) {
  const [volumeData, setVolumeData] = useState<ForecastVolumeData[]>([]);
  const [shiftCodes, setShiftCodes] = useState<ShiftCode[]>([]);
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [workingDaysRef, setWorkingDaysRef] = useState<WorkingDayRef[]>([]);
  const [roster, setRoster] = useState<EmployeeRoster[]>([]);
  const [loading, setLoading] = useState(false);
  const [showClearConfirm, setShowClearConfirm] = useState(false);
  const [viewMode, setViewMode] = useState<'grid' | 'calendar'>('grid');
  const [selectedEmployeeId, setSelectedEmployeeId] = useState<string | null>(null);
  const [selectedDateForAnalysis, setSelectedDateForAnalysis] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [skillFilter, setSkillFilter] = useState('all');
  const [shiftFilter, setShiftFilter] = useState('all');

  // Helper: Calculate rest hours between two shifts
  const getRestHours = (prevShift: ShiftCode, nextShift: ShiftCode) => {
    const [prevEndH, prevEndM] = prevShift.endTime.split(':').map(Number);
    const [nextStartH, nextStartM] = nextShift.startTime.split(':').map(Number);
    
    let endTotalMinutes = prevEndH * 60 + prevEndM;
    let startTotalMinutes = nextStartH * 60 + nextStartM;
    
    // If prev shift ends after midnight (e.g. 06:00), it's already in the "next day"
    // but for the purpose of rest calculation, we treat the next shift as being 24h later
    const [prevStartH] = prevShift.startTime.split(':').map(Number);
    if (prevEndH < prevStartH) {
       // Overnight shift
       return startTotalMinutes - endTotalMinutes;
    }
    
    // Normal shift: rest is (24h - end) + start
    return (24 * 60 - endTotalMinutes) + startTotalMinutes;
  };

  const getShiftBadgeClasses = (code: string, isRestViolation: boolean, isConsecViolation: boolean) => {
    if (code === 'OFF') return 'bg-slate-50 text-slate-300 border-slate-100';
    if (isRestViolation || isConsecViolation) {
      if (isRestViolation) return 'bg-red-50 text-red-700 border-red-300 ring-1 ring-red-200';
      return 'bg-orange-50 text-orange-700 border-orange-300 ring-1 ring-orange-200';
    }
    
    const idx = shiftCodes.findIndex(sc => sc.code === code);
    const colors = [
      'bg-blue-50 text-blue-700 border-blue-200',
      'bg-emerald-50 text-emerald-700 border-emerald-200',
      'bg-amber-50 text-amber-700 border-amber-200',
      'bg-purple-50 text-purple-700 border-purple-200',
      'bg-pink-50 text-pink-700 border-pink-200',
      'bg-cyan-50 text-cyan-700 border-cyan-200'
    ];
    return colors[idx % colors.length] || 'bg-indigo-50 text-indigo-700 border-indigo-200';
  };

  // Parameters for Erlang (synced from Firestore)
  const [aht, setAht] = useState(300);
  const [targetSL, setTargetSL] = useState(0.8);
  const [targetTime, setTargetTime] = useState(20);
  const [shrinkage, setShrinkage] = useState(0.3);

  const { totalDemand, dailyWeights } = React.useMemo(() => {
    const weights: Record<string, number> = {};
    let total = 0;
    volumeData.forEach(day => {
      let dayTotalHours = 0;
      Object.values(day.intervals).forEach(val => {
        const volume = Number(val) || 0;
        const res = calculateRequiredAgents(volume, aht, targetSL, targetTime);
        dayTotalHours += res.agents;
      });
      const weight = Math.ceil((dayTotalHours / 8) * (1 + shrinkage));
      weights[day.date] = weight;
      total += weight;
    });
    return { totalDemand: total, dailyWeights: weights };
  }, [volumeData, aht, targetSL, targetTime, shrinkage]);

  useEffect(() => {
    const unsubVolume = onSnapshot(query(collection(db, 'forecastVolume')), (snapshot) => {
      const list = snapshot.docs.map(doc => doc.data() as ForecastVolumeData);
      const sorted = list.sort((a, b) => a.date.localeCompare(b.date));
      setVolumeData(sorted);
      if (sorted.length > 0 && !selectedDateForAnalysis) {
        setSelectedDateForAnalysis(sorted[0].date);
      }
    });

    const unsubShifts = onSnapshot(query(collection(db, 'shiftCodes')), (snapshot) => {
      const list = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as ShiftCode));
      setShiftCodes(list);
    });

    const unsubEmployees = onSnapshot(query(collection(db, 'employees')), (snapshot) => {
      const list = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as Employee));
      setEmployees(list);
    });

    const unsubRef = onSnapshot(query(collection(db, 'workingDaysRef')), (snapshot) => {
      const list = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as WorkingDayRef));
      setWorkingDaysRef(list);
    });

    const unsubRoster = onSnapshot(doc(db, 'roster', 'current'), (snapshot) => {
      if (snapshot.exists()) {
        const data = snapshot.data();
        if (data.roster) setRoster(data.roster);
      }
    });

    const unsubSettings = onSnapshot(doc(db, 'erlangSettings', 'current'), (snapshot) => {
      if (snapshot.exists()) {
        const data = snapshot.data();
        if (data.aht) setAht(data.aht);
        if (data.targetSL) setTargetSL(data.targetSL / 100); // Erlang lib expects 0.8 for 80%
        if (data.targetTime) setTargetTime(data.targetTime);
        if (data.shrinkage) setShrinkage(data.shrinkage / 100);
      }
    });

    return () => {
      unsubVolume();
      unsubShifts();
      unsubEmployees();
      unsubRef();
      unsubRoster();
      unsubSettings();
    };
  }, []);

  const getJumpingAccuracy = () => {
    if (roster.length === 0 || volumeData.length < 2) return 100;
    
    let totalShifts = 0;
    let jumps = 0;
    
    roster.forEach(emp => {
      volumeData.forEach((day, idx) => {
        if (idx === 0) return;
        const prevDate = volumeData[idx - 1].date;
        const currShift = emp.days[day.date];
        const prevShift = emp.days[prevDate];
        
        if (currShift && currShift !== 'OFF' && prevShift && prevShift !== 'OFF') {
          totalShifts++;
          const sCurr = shiftCodes.find(sc => sc.code === currShift);
          const sPrev = shiftCodes.find(sc => sc.code === prevShift);
          if (sCurr && sPrev) {
            const [pH] = sPrev.startTime.split(':').map(Number);
            const [cH] = sCurr.startTime.split(':').map(Number);
            if (cH < pH) jumps++;
          }
        }
      });
    });
    
    if (totalShifts === 0) return 100;
    return Math.round(((totalShifts - jumps) / totalShifts) * 100);
  };

  const clearRoster = async () => {
    setLoading(true);
    try {
      await setDoc(doc(db, 'roster', 'current'), { roster: [] });
      setRoster([]);
      setShowClearConfirm(false);
      toast.success('Roster cleared successfully');
    } catch (error) {
      console.error(error);
      toast.error('Failed to clear roster');
    } finally {
      setLoading(false);
    }
  };

  const MetricCard = ({ title, value, icon, description, status }: { title: string, value: string, icon: React.ReactNode, description: string, status?: 'success' | 'warning' | 'danger' }) => (
    <Card className="border-none shadow-sm hover:shadow-md transition-shadow duration-200">
      <CardContent className="p-4">
        <div className="flex items-center justify-between mb-2">
          <div className="p-2 bg-slate-50 rounded-lg">
            {icon}
          </div>
          {status && (
            <div className={`w-2 h-2 rounded-full ${
              status === 'success' ? 'bg-emerald-500' : 
              status === 'warning' ? 'bg-amber-500' : 'bg-red-500'
            }`} />
          )}
        </div>
        <p className="text-[10px] uppercase font-bold text-slate-500 tracking-wider">{title}</p>
        <p className="text-xl font-bold text-slate-900 my-1">{value}</p>
        <p className="text-[10px] text-slate-400 font-medium">{description}</p>
      </CardContent>
    </Card>
  );

  const generateBalancedRoster = async () => {
    if (volumeData.length === 0 || shiftCodes.length === 0 || employees.length === 0) {
      toast.error('Missing data (Volume, Shifts, or Employees)');
      return;
    }

    setLoading(true);
    try {
      // 1. Use memoized daily requirements
      // (dailyWeights and totalDemand are available from component scope)

      // 2. Determine Target Working Days from Reference
      let targetWorkingDays = 22; 
      let targetOffDays = 9;
      let matchedMonth = "Default (22)";
      if (volumeData.length > 0) {
        const firstDate = new Date(volumeData[0].date);
        const monthStr = format(firstDate, 'MMM-yy');
        const ref = workingDaysRef.find(r => r.month.toLowerCase() === monthStr.toLowerCase());
        
        if (ref) {
          // Cap target working days at the actual forecast duration
          targetWorkingDays = Math.min(ref.workingDays, volumeData.length);
          // Target off days is whatever is left in the forecast period
          targetOffDays = volumeData.length - targetWorkingDays;
          matchedMonth = ref.month;

          if (volumeData.length < ref.totalDays) {
            toast.warning(`Forecast (${volumeData.length} days) is shorter than ${ref.month} (${ref.totalDays} days). Targets adjusted.`);
          }
        } else {
          targetWorkingDays = Math.min(22, volumeData.length);
          targetOffDays = volumeData.length - targetWorkingDays;
        }
      }
      toast.info(`Targets: ${targetWorkingDays}W / ${targetOffDays}O (${matchedMonth})`);

      // 3. Calculate Precision Headcounts
      // Total shifts available in the month for all employees
      const totalCapacity = employees.length * targetWorkingDays;
      const adjustedDailyHeadcounts: Record<string, number> = {};
      let assignedSoFar = 0;

      volumeData.forEach((day) => {
        // Start with the forecast requirement, but CAP at employee count
        const needed = dailyWeights[day.date] || 0;
        const capped = Math.min(needed, employees.length);
        adjustedDailyHeadcounts[day.date] = capped;
        assignedSoFar += capped;
      });

      // Adjust to match totalCapacity exactly
      let diff = totalCapacity - assignedSoFar;
      const sortedByWeight = [...volumeData].sort((a, b) => dailyWeights[b.date] - dailyWeights[a.date]);
      
      let safetyCounter = 0;
      while (diff !== 0 && safetyCounter < 100) {
        let changed = false;
        if (diff > 0) {
          // If we have extra capacity, add to busiest days first
          for (const day of sortedByWeight) {
            if (adjustedDailyHeadcounts[day.date] < employees.length) {
              adjustedDailyHeadcounts[day.date]++;
              diff--;
              changed = true;
            }
            if (diff === 0) break;
          }
        } else if (diff < 0) {
          // If we are over capacity, remove from least busy days first
          const leastBusy = [...sortedByWeight].reverse();
          for (const d of leastBusy) {
            if (adjustedDailyHeadcounts[d.date] > 0) {
              adjustedDailyHeadcounts[d.date]--;
              diff++;
              changed = true;
            }
            if (diff === 0) break;
          }
        }
        if (!changed) break;
        safetyCounter++;
      }

      // 4. Calculate Shift Distributions for each day based on adjusted headcount
      // ENHANCED: Greedy Coverage Optimizer to match interval-level demand
      const dailyRequirements: Record<string, Record<string, number>> = {};
      volumeData.forEach(day => {
        const headcount = adjustedDailyHeadcounts[day.date];
        const shiftSuggestions: Record<string, number> = {};
        shiftCodes.forEach(c => shiftSuggestions[c.code] = 0);

        // Calculate required agents per interval for this day
        const intervalRequirements: Record<string, number> = {};
        Object.entries(day.intervals).forEach(([interval, val]) => {
          const volume = Number(val) || 0;
          const res = calculateRequiredAgents(volume, aht, targetSL, targetTime);
          intervalRequirements[interval] = Math.ceil(res.agents * (1 + shrinkage));
        });

        const currentAssigned: Record<string, number> = {};
        Object.keys(intervalRequirements).forEach(int => currentAssigned[int] = 0);

        let assignedCount = 0;
        while (assignedCount < headcount) {
          let bestShift = null;
          let maxScore = -Infinity;

          for (const code of shiftCodes) {
            // Score = how much this shift reduces under-coverage
            let score = 0;
            Object.keys(intervalRequirements).forEach(interval => {
              if (isIntervalInShift(interval, code.startTime, code.endTime)) {
                const gap = intervalRequirements[interval] - currentAssigned[interval];
                if (gap > 0) {
                  score += gap * 3; // Even higher weight for filling gaps
                } else {
                  score -= (Math.abs(gap) + 1) * 6; // Even heavier penalty for over-coverage
                }
              }
            });

            if (score > maxScore) {
              maxScore = score;
              bestShift = code;
            }
          }

          if (bestShift) {
            shiftSuggestions[bestShift.code]++;
            Object.keys(intervalRequirements).forEach(interval => {
              if (isIntervalInShift(interval, bestShift!.startTime, bestShift!.endTime)) {
                currentAssigned[interval]++;
              }
            });
            assignedCount++;
          } else {
            break;
          }
        }
        dailyRequirements[day.date] = shiftSuggestions;
      });

      // 5. Initialize Roster and Shift Counters
      const newRoster: EmployeeRoster[] = employees.map(emp => ({
        employeeId: emp.id,
        employeeName: emp.name,
        nip: emp.nip,
        days: {},
        totalWorkingDays: 0,
        totalOffDays: 0,
        targetWorkingDays: targetWorkingDays,
        targetOffDays: targetOffDays
      }));

      const shiftCounts: Record<string, Record<string, number>> = {}; // employeeId -> shiftCode -> count
      employees.forEach(emp => {
        shiftCounts[emp.id] = {};
        shiftCodes.forEach(sc => {
          shiftCounts[emp.id][sc.code] = 0;
        });
      });

      // 6. Assign shifts day by day with enhanced "Best Match" logic for Jumping Accuracy and Coverage
      volumeData.forEach((day, dayIdx) => {
        const reqs = { ...dailyRequirements[day.date] };
        const prevDayDate = dayIdx > 0 ? volumeData[dayIdx - 1].date : null;
        const remainingDaysInMonth = volumeData.length - dayIdx;
        
        // Create a list of all required shift slots for today
        const shiftSlots: string[] = [];
        Object.entries(reqs).forEach(([code, count]) => {
          for (let i = 0; i < count; i++) shiftSlots.push(code);
        });

        const availableEmployees = [...newRoster].filter(emp => !emp.days[day.date]);
        
        // Helper to check consecutive days
        const getConsecutive = (emp: EmployeeRoster) => {
          let count = 0;
          for (let i = dayIdx - 1; i >= 0; i--) {
            const d = volumeData[i].date;
            if (emp.days[d] && emp.days[d] !== 'OFF') count++;
            else break;
          }
          return count;
        };

        // Helper to check consecutive off days
        const getConsecutiveOff = (emp: EmployeeRoster) => {
          let count = 0;
          for (let i = dayIdx - 1; i >= 0; i--) {
            const d = volumeData[i].date;
            if (emp.days[d] === 'OFF') count++;
            else break;
          }
          return count;
        };

        // Assign shifts using a greedy "Best Match" approach
        while (shiftSlots.length > 0 && availableEmployees.length > 0) {
          let bestMatch: { empIdx: number, slotIdx: number, score: number } | null = null;

          for (let eIdx = 0; eIdx < availableEmployees.length; eIdx++) {
            const emp = availableEmployees[eIdx];
            const consec = getConsecutive(emp);
            const consecOff = getConsecutiveOff(emp);
            const needed = emp.targetWorkingDays - emp.totalWorkingDays;
            const isCritical = needed >= remainingDaysInMonth;

            for (let sIdx = 0; sIdx < shiftSlots.length; sIdx++) {
              const shiftCode = shiftSlots[sIdx];
              const codeObj = shiftCodes.find(c => c.code === shiftCode)!;
              
              let score = 0;

              // 1. Hard Constraints (Rest Period & Labor Law)
              if (consec >= 6) score -= 5000; // Strong penalty for 6-day violation
              
              if (prevDayDate) {
                const prevShiftCode = emp.days[prevDayDate];
                if (prevShiftCode && prevShiftCode !== 'OFF') {
                  const prevCodeObj = shiftCodes.find(c => c.code === prevShiftCode);
                  if (prevCodeObj) {
                    const rest = getRestHours(prevCodeObj, codeObj);
                    if (rest < 11 * 60) score -= 10000; // Critical penalty for rest violation
                    
                    // 2. Forward Rotation (Jumping Accuracy)
                    const [prevH] = prevCodeObj.startTime.split(':').map(Number);
                    const [currH] = codeObj.startTime.split(':').map(Number);
                    if (currH < prevH) {
                      score -= 3000; // Even heavier penalty for backward jump
                    } else if (currH > prevH) {
                      score += 150; // Higher bonus for forward rotation
                    } else {
                      score += 80; // Higher bonus for same shift (stability)
                    }
                  }
                } else {
                  // Coming from OFF day - neutral but slightly positive
                  score += 50; 
                }
              }

              // 3. Monthly Target Adherence
              if (isCritical) score += 5000;
              score += needed * 50;

              // 4. Shift Balancing
              const shiftCount = shiftCounts[emp.employeeId][shiftCode] || 0;
              score -= shiftCount * 20;

              // 5. Off Day Balancing
              if (consecOff >= 2) score += 200;

              if (!bestMatch || score > bestMatch.score) {
                bestMatch = { empIdx: eIdx, slotIdx: sIdx, score };
              }
            }
          }

          if (bestMatch && bestMatch.score > -5000) {
            const emp = availableEmployees[bestMatch.empIdx];
            const shiftCode = shiftSlots[bestMatch.slotIdx];
            
            emp.days[day.date] = shiftCode;
            emp.totalWorkingDays++;
            shiftCounts[emp.employeeId][shiftCode] = (shiftCounts[emp.employeeId][shiftCode] || 0) + 1;
            
            shiftSlots.splice(bestMatch.slotIdx, 1);
            availableEmployees.splice(bestMatch.empIdx, 1);
          } else {
            // No more viable matches for today
            break;
          }
        }

        // Assign OFF to remaining employees
        availableEmployees.forEach(emp => {
          emp.days[day.date] = 'OFF';
          emp.totalOffDays++;
        });

        // 6.5 Daily Jump Optimizer: Swap shifts on the same day to minimize jumps
        if (prevDayDate) {
          let dailySwapSafety = 0;
          let swappedThisDay = true;
          while (swappedThisDay && dailySwapSafety < 5) {
            swappedThisDay = false;
            dailySwapSafety++;
            for (let i = 0; i < newRoster.length; i++) {
              for (let j = i + 1; j < newRoster.length; j++) {
                const emp1 = newRoster[i];
                const emp2 = newRoster[j];
                const shift1 = emp1.days[day.date];
                const shift2 = emp2.days[day.date];

                if (shift1 !== 'OFF' && shift2 !== 'OFF' && shift1 !== shift2) {
                  const sCode1 = shiftCodes.find(c => c.code === shift1)!;
                  const sCode2 = shiftCodes.find(c => c.code === shift2)!;

                  const getJumps = (emp: EmployeeRoster, sCode: ShiftCode) => {
                    const prev = emp.days[prevDayDate];
                    if (!prev || prev === 'OFF') return 0;
                    const prevS = shiftCodes.find(c => c.code === prev)!;
                    const [pH] = prevS.startTime.split(':').map(Number);
                    const [cH] = sCode.startTime.split(':').map(Number);
                    return cH < pH ? 1 : 0;
                  };

                  const checkRest = (emp: EmployeeRoster, sCode: ShiftCode) => {
                    const prev = emp.days[prevDayDate];
                    if (prev && prev !== 'OFF') {
                      const sPrev = shiftCodes.find(sc => sc.code === prev);
                      if (sPrev && getRestHours(sPrev, sCode) < 11 * 60) return false;
                    }
                    return true;
                  };

                  const currentJumps = getJumps(emp1, sCode1) + getJumps(emp2, sCode2);
                  const swappedJumps = getJumps(emp1, sCode2) + getJumps(emp2, sCode1);

                  if (swappedJumps < currentJumps && checkRest(emp1, sCode2) && checkRest(emp2, sCode1)) {
                    emp1.days[day.date] = shift2;
                    emp2.days[day.date] = shift1;
                    shiftCounts[emp1.employeeId][shift1]--;
                    shiftCounts[emp1.employeeId][shift2]++;
                    shiftCounts[emp2.employeeId][shift2]--;
                    shiftCounts[emp2.employeeId][shift1]++;
                    swappedThisDay = true;
                  }
                }
              }
            }
          }

          // 6.6 Daily Fairness Balancer: Swap shifts to improve even distribution IF it doesn't create jumps
          let fairnessSafety = 0;
          let fairnessSwapped = true;
          while (fairnessSwapped && fairnessSafety < 5) {
            fairnessSwapped = false;
            fairnessSafety++;
            for (let i = 0; i < newRoster.length; i++) {
              for (let j = i + 1; j < newRoster.length; j++) {
                const emp1 = newRoster[i];
                const emp2 = newRoster[j];
                const shift1 = emp1.days[day.date];
                const shift2 = emp2.days[day.date];

                if (shift1 !== 'OFF' && shift2 !== 'OFF' && shift1 !== shift2) {
                  const sCode1 = shiftCodes.find(c => c.code === shift1)!;
                  const sCode2 = shiftCodes.find(c => c.code === shift2)!;

                  const aCount1 = shiftCounts[emp1.employeeId][shift1] || 0;
                  const aCount2 = shiftCounts[emp1.employeeId][shift2] || 0;
                  const bCount1 = shiftCounts[emp2.employeeId][shift1] || 0;
                  const bCount2 = shiftCounts[emp2.employeeId][shift2] || 0;

                  // Fairness score: we want counts to be as close as possible
                  const currentDiff = Math.abs(aCount1 - bCount1) + Math.abs(aCount2 - bCount2);
                  const swappedDiff = Math.abs((aCount1 - 1) - (bCount1 + 1)) + Math.abs((aCount2 + 1) - (bCount2 - 1));

                  if (swappedDiff < currentDiff) {
                    // Check if swap is safe (No Jumps, No Rest Violations)
                    const getJumps = (emp: EmployeeRoster, sCode: ShiftCode) => {
                      const prev = emp.days[prevDayDate];
                      if (!prev || prev === 'OFF') return 0;
                      const prevS = shiftCodes.find(c => c.code === prev)!;
                      const [pH] = prevS.startTime.split(':').map(Number);
                      const [cH] = sCode.startTime.split(':').map(Number);
                      return cH < pH ? 1 : 0;
                    };
                    const checkRest = (emp: EmployeeRoster, sCode: ShiftCode) => {
                      const prev = emp.days[prevDayDate];
                      if (prev && prev !== 'OFF') {
                        const sPrev = shiftCodes.find(sc => sc.code === prev);
                        if (sPrev && getRestHours(sPrev, sCode) < 11 * 60) return false;
                      }
                      return true;
                    };

                    const currentJumps = getJumps(emp1, sCode1) + getJumps(emp2, sCode2);
                    const swappedJumps = getJumps(emp1, sCode2) + getJumps(emp2, sCode1);

                    if (swappedJumps <= currentJumps && checkRest(emp1, sCode2) && checkRest(emp2, sCode1)) {
                      emp1.days[day.date] = shift2;
                      emp2.days[day.date] = shift1;
                      shiftCounts[emp1.employeeId][shift1]--;
                      shiftCounts[emp1.employeeId][shift2]++;
                      shiftCounts[emp2.employeeId][shift2]--;
                      shiftCounts[emp2.employeeId][shift1]++;
                      fairnessSwapped = true;
                    }
                  }
                }
              }
            }
          }
        }
      });

      // 7. Post-Processing Rebalancer for 100% Precision
      // If someone is over and someone is under, swap shifts on days where the under-target person is OFF
      let rebalanceSafety = 0;
      
      const getMaxStreak = (emp: EmployeeRoster) => {
        let max = 0;
        let current = 0;
        volumeData.forEach(day => {
          if (emp.days[day.date] !== 'OFF') {
            current++;
            if (current > max) max = current;
          } else {
            current = 0;
          }
        });
        return max;
      };

      while (rebalanceSafety < 1000) {
        const overTarget = newRoster
          .filter(e => e.totalWorkingDays > e.targetWorkingDays)
          .sort((a, b) => {
            const aStreak = getMaxStreak(a);
            const bStreak = getMaxStreak(b);
            if (aStreak >= 6 && bStreak < 6) return -1;
            if (aStreak < 6 && bStreak >= 6) return 1;
            return b.totalWorkingDays - a.totalWorkingDays;
          })[0];
          
        const underTarget = newRoster
          .filter(e => e.totalWorkingDays < e.targetWorkingDays)
          .sort((a, b) => a.totalWorkingDays - b.totalWorkingDays)[0];
        
        if (!overTarget || !underTarget) break;

        // Find a day where overTarget worked and underTarget was OFF
        let swapped = false;
        
        const daysToTry = [...volumeData].sort((a, b) => {
          const aIsStreak = overTarget.days[a.date] !== 'OFF';
          const bIsStreak = overTarget.days[b.date] !== 'OFF';
          return aIsStreak === bIsStreak ? 0 : (aIsStreak ? -1 : 1);
        });

        // STAGE 1: Try to find a swap that doesn't violate rest rules AND consecutive rules
        for (const day of daysToTry) {
          const overShift = overTarget.days[day.date];
          const underShift = underTarget.days[day.date];
          if (overShift !== 'OFF' && underShift === 'OFF') {
            const dayIdx = volumeData.findIndex(d => d.date === day.date);
            const sCurr = shiftCodes.find(sc => sc.code === overShift);
            if (!sCurr) continue;

            let restOk = true;
            let isJump = false;
            if (dayIdx > 0) {
              const prevDate = volumeData[dayIdx - 1].date;
              const prevShift = underTarget.days[prevDate];
              if (prevShift && prevShift !== 'OFF') {
                const sPrev = shiftCodes.find(sc => sc.code === prevShift);
                if (sPrev) {
                  if (getRestHours(sPrev, sCurr) < 11 * 60) restOk = false;
                  const [pH] = sPrev.startTime.split(':').map(Number);
                  const [cH] = sCurr.startTime.split(':').map(Number);
                  if (cH < pH) isJump = true;
                }
              }
            }
            if (restOk && dayIdx < volumeData.length - 1) {
              const nextDate = volumeData[dayIdx + 1].date;
              const nextShift = underTarget.days[nextDate];
              if (nextShift && nextShift !== 'OFF') {
                const sNext = shiftCodes.find(sc => sc.code === nextShift);
                if (sNext) {
                  if (getRestHours(sCurr, sNext) < 11 * 60) restOk = false;
                  const [cH] = sCurr.startTime.split(':').map(Number);
                  const [nH] = sNext.startTime.split(':').map(Number);
                  if (nH < cH) isJump = true;
                }
              }
            }

            let consecOk = true;
            let count = 0;
            for (let i = dayIdx - 1; i >= 0; i--) {
              if (underTarget.days[volumeData[i].date] !== 'OFF') count++;
              else break;
            }
            for (let i = dayIdx + 1; i < volumeData.length; i++) {
              if (underTarget.days[volumeData[i].date] !== 'OFF') count++;
              else break;
            }
            if (count >= 5) consecOk = false;

            if (restOk && consecOk && !isJump) {
              underTarget.days[day.date] = overShift;
              overTarget.days[day.date] = 'OFF';
              overTarget.totalWorkingDays--; overTarget.totalOffDays++;
              underTarget.totalWorkingDays++; underTarget.totalOffDays--;
              swapped = true; break;
            }
          }
        }

        // STAGE 1.5: Try to find a swap that satisfies rest and consecutive rules (even if it's a jump)
        if (!swapped) {
          for (const day of daysToTry) {
            const overShift = overTarget.days[day.date];
            const underShift = underTarget.days[day.date];
            if (overShift !== 'OFF' && underShift === 'OFF') {
              const dayIdx = volumeData.findIndex(d => d.date === day.date);
              const sCurr = shiftCodes.find(sc => sc.code === overShift);
              if (!sCurr) continue;

              let restOk = true;
              if (dayIdx > 0) {
                const prevDate = volumeData[dayIdx - 1].date;
                const prevShift = underTarget.days[prevDate];
                if (prevShift && prevShift !== 'OFF') {
                  const sPrev = shiftCodes.find(sc => sc.code === prevShift);
                  if (sPrev && getRestHours(sPrev, sCurr) < 11 * 60) restOk = false;
                }
              }
              if (restOk && dayIdx < volumeData.length - 1) {
                const nextDate = volumeData[dayIdx + 1].date;
                const nextShift = underTarget.days[nextDate];
                if (nextShift && nextShift !== 'OFF') {
                  const sNext = shiftCodes.find(sc => sc.code === nextShift);
                  if (sNext && getRestHours(sCurr, sNext) < 11 * 60) restOk = false;
                }
              }

              let consecOk = true;
              let count = 0;
              for (let i = dayIdx - 1; i >= 0; i--) {
                if (underTarget.days[volumeData[i].date] !== 'OFF') count++;
                else break;
              }
              for (let i = dayIdx + 1; i < volumeData.length; i++) {
                if (underTarget.days[volumeData[i].date] !== 'OFF') count++;
                else break;
              }
              if (count >= 5) consecOk = false;

              if (restOk && consecOk) {
                underTarget.days[day.date] = overShift;
                overTarget.days[day.date] = 'OFF';
                overTarget.totalWorkingDays--; overTarget.totalOffDays++;
                underTarget.totalWorkingDays++; underTarget.totalOffDays--;
                swapped = true; break;
              }
            }
          }
        }

        // STAGE 2: Try to find a swap that satisfies Consecutive rule (Primary Rule)
        if (!swapped) {
          for (const day of daysToTry) {
            const overShift = overTarget.days[day.date];
            const underShift = underTarget.days[day.date];
            if (overShift !== 'OFF' && underShift === 'OFF') {
              const dayIdx = volumeData.findIndex(d => d.date === day.date);
              let count = 0;
              for (let i = dayIdx - 1; i >= 0; i--) {
                if (underTarget.days[volumeData[i].date] !== 'OFF') count++;
                else break;
              }
              for (let i = dayIdx + 1; i < volumeData.length; i++) {
                if (underTarget.days[volumeData[i].date] !== 'OFF') count++;
                else break;
              }
              if (count < 5) {
                underTarget.days[day.date] = overShift;
                overTarget.days[day.date] = 'OFF';
                overTarget.totalWorkingDays--; overTarget.totalOffDays++;
                underTarget.totalWorkingDays++; underTarget.totalOffDays--;
                swapped = true; break;
              }
            }
          }
        }

        // STAGE 3: Forced swap to maintain precision (Secondary Priority)
        // We only do this if we absolutely must hit the target and no safe swaps exist
        if (!swapped) {
          for (const day of daysToTry) {
            const overShift = overTarget.days[day.date];
            const underShift = underTarget.days[day.date];
            if (overShift !== 'OFF' && underShift === 'OFF') {
              underTarget.days[day.date] = overShift;
              overTarget.days[day.date] = 'OFF';
              overTarget.totalWorkingDays--; overTarget.totalOffDays++;
              underTarget.totalWorkingDays++; underTarget.totalOffDays--;
              swapped = true; break;
            }
          }
        }

        if (!swapped) break; 
        rebalanceSafety++;
      }

      // 8. Consecutive Day Optimizer
      // Try to break up 6+ day streaks by shifting a shift to a nearby OFF day for the SAME employee
      newRoster.forEach(emp => {
        let consecutive = 0;
        let streakStartIdx = -1;
        
        for (let i = 0; i < volumeData.length; i++) {
          if (emp.days[volumeData[i].date] !== 'OFF') {
            if (consecutive === 0) streakStartIdx = i;
            consecutive++;
            
            if (consecutive >= 6) {
              // We have a streak of 6 or more. Try to move one shift to a nearby OFF day.
              let moved = false;
              // Look for an OFF day within +/- 3 days of the streak
              const searchRange = 5;
              for (let offset = -searchRange; offset <= searchRange; offset++) {
                const targetIdx = i + offset;
                if (targetIdx >= 0 && targetIdx < volumeData.length && emp.days[volumeData[targetIdx].date] === 'OFF') {
                  // Check if moving the shift from 'i' to 'targetIdx' is safe
                  const shiftToMove = emp.days[volumeData[i].date];
                  
                  // Simple check: would targetIdx create a new streak?
                  let newStreakCount = 1;
                  for (let j = targetIdx - 1; j >= 0; j--) {
                    if (emp.days[volumeData[j].date] !== 'OFF') newStreakCount++;
                    else break;
                  }
                  for (let j = targetIdx + 1; j < volumeData.length; j++) {
                    if (emp.days[volumeData[j].date] !== 'OFF') newStreakCount++;
                    else break;
                  }
                  
                  if (newStreakCount <= 5) {
                    // Perform the move
                    emp.days[volumeData[targetIdx].date] = shiftToMove;
                    emp.days[volumeData[i].date] = 'OFF';
                    moved = true;
                    // Reset streak detection for this employee
                    consecutive = 0;
                    i = Math.max(-1, streakStartIdx - 1); 
                    break;
                  }
                }
              }
              if (!moved) consecutive = 0; // Give up on this streak
            }
          } else {
            consecutive = 0;
          }
        }
      });
      
      await setDoc(doc(db, 'roster', 'current'), { roster: newRoster });
      setRoster(newRoster);
      
      const precisionCount = newRoster.filter(e => e.totalWorkingDays === e.targetWorkingDays).length;
      const precisionPct = Math.round((precisionCount / newRoster.length) * 100);

      // 6-Day Compliance Check (Max 5 consecutive)
      let complianceViolations = 0;
      newRoster.forEach(emp => {
        let consecutive = 0;
        volumeData.forEach(day => {
          if (emp.days[day.date] !== 'OFF') {
            consecutive++;
            if (consecutive >= 6) complianceViolations++;
          } else {
            consecutive = 0;
          }
        });
      });
      const compliancePct = Math.round(((newRoster.length - complianceViolations) / newRoster.length) * 100);

      if (precisionPct < 100) {
        toast.warning(`Roster precision is at ${precisionPct}%. Some targets couldn't be met due to constraints.`);
      } else {
        toast.success(`Balanced roster generated with 100% precision and ${compliancePct}% 6-day compliance!`);
      }
    } catch (error) {
      console.error(error);
      toast.error('Failed to generate roster');
    } finally {
      setLoading(false);
    }
  };

  function isIntervalInShift(interval: string, shiftStart: string, shiftEnd: string): boolean {
    try {
      const [intStartStr] = interval.split(' - ');
      const [intH] = intStartStr.split(':').map(Number);
      const [startH] = shiftStart.split(':').map(Number);
      const [endH] = shiftEnd.split(':').map(Number);
      if (startH < endH) return intH >= startH && intH < endH;
      return intH >= startH || intH < endH;
    } catch (e) {
      return false;
    }
  }

  const downloadRoster = () => {
    if (roster.length === 0) return;
    const dates = volumeData.map(d => d.date);
    
    // Header
    const headers = ['NIP', 'Name', 'Skill', ...dates, 'Total Work', 'Total Off'];
    
    // Employee Rows
    const rows = roster.map(emp => {
      const employeeInfo = employees.find(e => e.id === emp.employeeId);
      return [
        emp.nip,
        emp.employeeName,
        employeeInfo?.skill || '-',
        ...dates.map(d => emp.days[d] || 'OFF'),
        emp.totalWorkingDays,
        emp.totalOffDays
      ];
    });

    // Summary Section
    const summaryRows = [
      [],
      ['ROSTER SUMMARY'],
      ['Metric', 'Value'],
      ['Total Employees', roster.length],
      ['Total Forecast Days', volumeData.length],
      ['Avg Working Days', (roster.reduce((acc, curr) => acc + curr.totalWorkingDays, 0) / roster.length).toFixed(1)],
      ['Interval Coverage Accuracy', `${getIntervalAccuracy()}%`],
      ['Jumping Accuracy', `${getJumpingAccuracy()}%`],
      [],
      ['SHIFT DISTRIBUTION SUMMARY'],
      ['Shift Code', 'Total Assignments']
    ];

    shiftCodes.forEach(code => {
      let count = 0;
      roster.forEach(emp => {
        Object.values(emp.days).forEach(d => {
          if (d === code.code) count++;
        });
      });
      summaryRows.push([code.code, count]);
    });

    const csvContent = [headers, ...rows, ...summaryRows].map(r => r.join(';')).join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `enhanced_roster_${format(new Date(), 'yyyyMMdd')}.csv`;
    a.click();
  };

  const PersonalCalendar = ({ employee }: { employee: EmployeeRoster }) => {
    if (!volumeData.length) return null;
    
    const firstDate = new Date(volumeData[0].date);
    const monthStart = new Date(firstDate.getFullYear(), firstDate.getMonth(), 1);
    const monthEnd = new Date(firstDate.getFullYear(), firstDate.getMonth() + 1, 0);
    
    const daysInMonth = [];
    let curr = new Date(monthStart);
    // Pad start
    const startDay = curr.getDay(); // 0 is Sunday
    for (let i = 0; i < startDay; i++) {
      daysInMonth.push(null);
    }
    // Actual days
    while (curr <= monthEnd) {
      daysInMonth.push(new Date(curr));
      curr.setDate(curr.getDate() + 1);
    }
    
    return (
      <div className="space-y-4 p-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-bold">
              {employee.employeeName.charAt(0)}
            </div>
            <div>
              <h3 className="font-bold text-slate-900">{employee.employeeName}</h3>
              <p className="text-xs text-slate-500 font-mono">{employee.nip}</p>
            </div>
          </div>
          <div className="flex gap-4 text-xs">
            <div className="flex items-center gap-1.5">
              <div className="w-3 h-3 rounded-full bg-emerald-500" />
              <span>Working</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="w-3 h-3 rounded-full bg-slate-200" />
              <span>OFF</span>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-7 gap-px bg-slate-200 rounded-lg overflow-hidden border border-slate-200">
          {['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].map(d => (
            <div key={d} className="bg-slate-50 p-2 text-center text-[10px] font-bold text-slate-500 uppercase">
              {d}
            </div>
          ))}
          {daysInMonth.map((date, idx) => {
            if (!date) return <div key={`pad-${idx}`} className="bg-slate-50/50 h-24" />;
            
            const dateStr = format(date, 'yyyy-MM-dd');
            const shift = employee.days[dateStr] || 'OFF';
            const isToday = isSameDay(date, new Date());
            
            // Check violations
            let restViolation = false;
            let consecViolation = false;
            const dayIdx = volumeData.findIndex(d => d.date === dateStr);
            if (shift !== 'OFF' && dayIdx !== -1) {
              const prevDayDate = dayIdx > 0 ? volumeData[dayIdx - 1].date : null;
              const prevShiftCode = prevDayDate ? employee.days[prevDayDate] : null;
              if (prevShiftCode && prevShiftCode !== 'OFF') {
                const sPrev = shiftCodes.find(sc => sc.code === prevShiftCode);
                const sCurr = shiftCodes.find(sc => sc.code === shift);
                if (sPrev && sCurr && getRestHours(sPrev, sCurr) < 11 * 60) restViolation = true;
              }
              let count = 1;
              for (let i = dayIdx - 1; i >= 0; i--) {
                if (employee.days[volumeData[i].date] !== 'OFF') count++;
                else break;
              }
              if (count >= 6) consecViolation = true;
            }

            return (
              <div 
                key={dateStr} 
                className={`bg-white h-24 p-2 relative group transition-colors hover:bg-slate-50 ${isToday ? 'ring-1 ring-inset ring-indigo-500 z-10' : ''}`}
              >
                <span className={`text-xs font-medium ${isToday ? 'text-indigo-600 font-bold' : 'text-slate-400'}`}>
                  {date.getDate()}
                </span>
                <div className="mt-2">
                  {shift !== 'OFF' ? (
                    <div className={`rounded-md p-1.5 text-[10px] font-bold text-center shadow-sm ${getShiftBadgeClasses(shift, restViolation, consecViolation)}`}>
                      {shift}
                      {(restViolation || consecViolation) && (
                        <AlertCircle className="w-3 h-3 inline ml-1 text-current opacity-70" />
                      )}
                    </div>
                  ) : (
                    <div className="rounded-md p-1.5 text-[10px] font-medium text-center text-slate-300 bg-slate-50 border border-dashed border-slate-200">
                      OFF
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  const getIntervalAccuracy = () => {
    if (roster.length === 0 || volumeData.length === 0) return 0;
    
    let totalRequired = 0;
    let totalAbsDiff = 0;

    volumeData.forEach(day => {
      const dayReqs: Record<string, number> = {};
      Object.entries(day.intervals).forEach(([interval, val]) => {
        const volume = Number(val) || 0;
        const res = calculateRequiredAgents(volume, aht, targetSL, targetTime);
        const req = Math.ceil(res.agents * (1 + shrinkage));
        dayReqs[interval] = req;
        totalRequired += req;
      });

      const dayAssigned: Record<string, number> = {};
      Object.keys(dayReqs).forEach(int => dayAssigned[int] = 0);

      roster.forEach(emp => {
        const shiftCode = emp.days[day.date];
        if (shiftCode && shiftCode !== 'OFF') {
          const code = shiftCodes.find(c => c.code === shiftCode);
          if (code) {
            Object.keys(dayReqs).forEach(interval => {
              if (isIntervalInShift(interval, code.startTime, code.endTime)) {
                dayAssigned[interval]++;
              }
            });
          }
        }
      });

      Object.keys(dayReqs).forEach(interval => {
        totalAbsDiff += Math.abs(dayReqs[interval] - dayAssigned[interval]);
      });
    });

    if (totalRequired === 0) return 100;
    return Math.round(Math.max(0, (1 - (totalAbsDiff / (totalRequired * 2))) * 100));
  };

  const getCoverageDataForSelectedDate = () => {
    if (!selectedDateForAnalysis || volumeData.length === 0) return [];
    const day = volumeData.find(d => d.date === selectedDateForAnalysis);
    if (!day) return [];

    const dayReqs: Record<string, number> = {};
    Object.entries(day.intervals).forEach(([interval, val]) => {
      const volume = Number(val) || 0;
      const res = calculateRequiredAgents(volume, aht, targetSL, targetTime);
      dayReqs[interval] = Math.ceil(res.agents * (1 + shrinkage));
    });

    const dayAssigned: Record<string, number> = {};
    Object.keys(dayReqs).forEach(int => dayAssigned[int] = 0);

    roster.forEach(emp => {
      const shiftCode = emp.days[day.date];
      if (shiftCode && shiftCode !== 'OFF') {
        const code = shiftCodes.find(c => c.code === shiftCode);
        if (code) {
          Object.keys(dayReqs).forEach(interval => {
            if (isIntervalInShift(interval, code.startTime, code.endTime)) {
              dayAssigned[interval]++;
            }
          });
        }
      }
    });

    return Object.keys(dayReqs).sort().map(interval => ({
      interval,
      Required: dayReqs[interval],
      Assigned: dayAssigned[interval]
    }));
  };

  const filteredRoster = roster.filter(emp => {
    const employeeInfo = employees.find(e => e.id === emp.employeeId);
    const matchesSearch = emp.employeeName.toLowerCase().includes(searchTerm.toLowerCase()) || 
                          emp.nip.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesSkill = skillFilter === 'all' || employeeInfo?.skill === skillFilter;
    
    let matchesShift = true;
    if (shiftFilter !== 'all') {
      matchesShift = Object.values(emp.days).includes(shiftFilter);
    }

    return matchesSearch && matchesSkill && matchesShift;
  });

  const uniqueSkills = Array.from(new Set(employees.map(e => e.skill))).filter(Boolean);

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-slate-900">Roster Management</h1>
          <p className="text-slate-500">Generate and manage balanced employee schedules.</p>
        </div>
        <div className="flex items-center gap-2">
          <div className="flex bg-slate-100 p-1 rounded-lg mr-2">
            <Button 
              variant="ghost" 
              size="sm" 
              onClick={() => setViewMode('grid')}
              className={`text-xs h-7 px-3 ${viewMode === 'grid' ? 'bg-white shadow-sm' : 'text-slate-500'}`}
            >
              Grid
            </Button>
            <Button 
              variant="ghost" 
              size="sm" 
              onClick={() => {
                setViewMode('calendar');
                if (!selectedEmployeeId && roster.length > 0) setSelectedEmployeeId(roster[0].employeeId);
              }}
              className={`text-xs h-7 px-3 ${viewMode === 'calendar' ? 'bg-white shadow-sm' : 'text-slate-500'}`}
            >
              Calendar
            </Button>
          </div>
          {isAdmin && (
            <div className="flex gap-2">
              {showClearConfirm ? (
                <div className="flex gap-1 animate-in fade-in slide-in-from-right-2 duration-200">
                  <Button variant="destructive" size="sm" onClick={clearRoster} disabled={loading}>Confirm Clear</Button>
                  <Button variant="ghost" size="sm" onClick={() => setShowClearConfirm(false)} disabled={loading}>Cancel</Button>
                </div>
              ) : (
                <Button 
                  variant="outline" 
                  onClick={() => setShowClearConfirm(true)} 
                  disabled={loading || roster.length === 0}
                  className="text-slate-500 hover:text-red-600 hover:bg-red-50 border-slate-200"
                >
                  Clear Roster
                </Button>
              )}
              <Button onClick={generateBalancedRoster} disabled={loading} className="gap-2 bg-indigo-600 hover:bg-indigo-700 text-white">
                {loading ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
                Generate Balanced Roster
              </Button>
            </div>
          )}
          <Button variant="outline" onClick={downloadRoster} disabled={roster.length === 0} className="gap-2">
            <Download className="w-4 h-4" />
            Export CSV
          </Button>
        </div>
      </div>

      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 bg-slate-50 p-4 rounded-xl border border-slate-100">
        <div className="flex flex-1 items-center gap-2">
          <div className="relative flex-1 max-w-sm">
            <Activity className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <input 
              type="text"
              placeholder="Search name or NIP..."
              className="w-full pl-10 pr-4 py-2 bg-white border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>
          <select 
            className="bg-white border border-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
            value={skillFilter}
            onChange={(e) => setSkillFilter(e.target.value)}
          >
            <option value="all">All Skills</option>
            {uniqueSkills.map(skill => (
              <option key={skill} value={skill}>{skill}</option>
            ))}
          </select>
          <select 
            className="bg-white border border-slate-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
            value={shiftFilter}
            onChange={(e) => setShiftFilter(e.target.value)}
          >
            <option value="all">All Shifts</option>
            {shiftCodes.map(shift => (
              <option key={shift.code} value={shift.code}>{shift.code}</option>
            ))}
          </select>
        </div>
        <div className="text-xs text-slate-500 font-medium">
          Showing {filteredRoster.length} of {roster.length} employees
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-1 border-none shadow-sm bg-gradient-to-br from-indigo-600 to-blue-700 text-white overflow-hidden relative">
          <div className="absolute top-0 right-0 p-4 opacity-10">
            <TrendingUp className="w-24 h-24" />
          </div>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-indigo-100 flex items-center gap-2">
              <Activity className="w-4 h-4" />
              Capacity Overview
            </CardTitle>
          </CardHeader>
          <CardContent>
            {roster.length === 0 ? (
              <div className="py-8 flex flex-col items-center justify-center text-indigo-200 gap-2">
                <CalendarIcon className="w-8 h-8 opacity-50" />
                <p className="text-xs font-medium">No data to analyze</p>
              </div>
            ) : (
              <div className="space-y-4">
                <div>
                  <div className="flex justify-between items-end mb-1">
                    <span className="text-2xl font-bold">
                      {Math.round(((roster.length * (roster[0]?.targetWorkingDays || 0)) / (totalDemand || 1)) * 100)}%
                    </span>
                    <span className="text-xs text-indigo-100 font-medium">Coverage</span>
                  </div>
                  <div className="w-full bg-white/20 h-2 rounded-full overflow-hidden backdrop-blur-sm">
                    <div 
                      className="h-full bg-white transition-all duration-500 ease-out"
                      style={{ 
                        width: `${Math.min(100, (roster.length * (roster[0]?.targetWorkingDays || 0)) / (totalDemand || 1) * 100)}%` 
                      }}
                    />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4 pt-2">
                  <div className="bg-white/10 rounded-lg p-2 backdrop-blur-sm">
                    <p className="text-[10px] uppercase font-bold text-indigo-100 mb-1">Demand</p>
                    <p className="text-lg font-bold">{totalDemand}</p>
                  </div>
                  <div className="bg-white/10 rounded-lg p-2 backdrop-blur-sm">
                    <p className="text-[10px] uppercase font-bold text-indigo-100 mb-1">Supply</p>
                    <p className="text-lg font-bold">{roster.length * (roster[0]?.targetWorkingDays || 0)}</p>
                  </div>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <div className="lg:col-span-2 space-y-6">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <MetricCard 
              title="Monthly Target" 
              value={roster.length > 0 ? `${roster[0]?.targetWorkingDays}W / ${roster[0]?.targetOffDays}O` : '-'}
              icon={<Target className="w-4 h-4 text-blue-500" />}
              description="Standard work/off ratio"
            />
            <MetricCard 
              title="Roster Precision" 
              value={roster.length > 0 ? `${Math.round((roster.filter(e => e.totalWorkingDays === e.targetWorkingDays && e.totalOffDays === e.targetOffDays).length / roster.length) * 100)}%` : '-'}
              icon={<CheckCircle2 className="w-4 h-4 text-emerald-500" />}
              description="Target adherence"
              status={roster.length > 0 ? (roster.every(e => e.totalWorkingDays === e.targetWorkingDays && e.totalOffDays === e.targetOffDays) ? 'success' : 'warning') : undefined}
            />
            <MetricCard 
              title="6-Day Compliance" 
              value={roster.length > 0 ? `${(() => {
                let violations = 0;
                roster.forEach(emp => {
                  let consecutive = 0;
                  volumeData.forEach(day => {
                    if (emp.days[day.date] !== 'OFF') {
                      consecutive++;
                      if (consecutive >= 6) {
                        violations++;
                        return;
                      }
                    } else {
                      consecutive = 0;
                    }
                  });
                });
                return Math.round(((roster.length - violations) / roster.length) * 100);
              })()}%` : '-'}
              icon={<ShieldCheck className="w-4 h-4 text-orange-500" />}
              description="Labor law safety"
              status={roster.length > 0 ? ((() => {
                let violations = 0;
                roster.forEach(emp => {
                  let consecutive = 0;
                  volumeData.forEach(day => {
                    if (emp.days[day.date] !== 'OFF') {
                      consecutive++;
                      if (consecutive >= 6) {
                        violations++;
                        return;
                      }
                    } else {
                      consecutive = 0;
                    }
                  });
                });
                return violations === 0;
              })() ? 'success' : 'warning') : undefined}
            />
            <MetricCard 
              title="Jumping Accuracy" 
              value={roster.length > 0 ? `${getJumpingAccuracy()}%` : '-'}
              icon={<Zap className="w-4 h-4 text-amber-500" />}
              description="Forward rotation"
              status={roster.length > 0 ? (getJumpingAccuracy() >= 95 ? 'success' : getJumpingAccuracy() >= 80 ? 'warning' : 'danger') : undefined}
            />
          </div>

          <div className="grid grid-cols-1 gap-4">
            <MetricCard 
              title="Interval Coverage Accuracy" 
              value={roster.length > 0 ? `${getIntervalAccuracy()}%` : '-'}
              icon={<BarChart3 className="w-4 h-4 text-indigo-500" />}
              description="Staff vs Demand per interval"
              status={roster.length > 0 ? (getIntervalAccuracy() >= 90 ? 'success' : getIntervalAccuracy() >= 75 ? 'warning' : 'danger') : undefined}
            />
          </div>
        </div>
      </div>

      {roster.length > 0 && selectedDateForAnalysis && (
        <Card className="border-none shadow-sm overflow-hidden">
          <CardHeader className="border-b border-slate-100 flex flex-row items-center justify-between py-4">
            <div>
              <CardTitle className="text-lg flex items-center gap-2">
                <Activity className="w-5 h-5 text-indigo-600" />
                Interval Coverage Analysis
              </CardTitle>
              <CardDescription>
                Required vs Assigned staff for {format(new Date(selectedDateForAnalysis), 'EEEE, MMMM d, yyyy')}
              </CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-xs font-medium text-slate-500">Select Date:</span>
              <select 
                className="text-xs border border-slate-200 rounded px-2 py-1 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
                value={selectedDateForAnalysis}
                onChange={(e) => setSelectedDateForAnalysis(e.target.value)}
              >
                {volumeData.map(day => (
                  <option key={day.date} value={day.date}>
                    {format(new Date(day.date), 'MMM d')}
                  </option>
                ))}
              </select>
            </div>
          </CardHeader>
          <CardContent className="pt-6">
            <div className="h-[300px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={getCoverageDataForSelectedDate()}>
                  <defs>
                    <linearGradient id="colorRequired" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#6366f1" stopOpacity={0.1}/>
                      <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
                    </linearGradient>
                    <linearGradient id="colorAssigned" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#10b981" stopOpacity={0.1}/>
                      <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                  <XAxis 
                    dataKey="interval" 
                    axisLine={false}
                    tickLine={false}
                    tick={{ fontSize: 10, fill: '#64748b' }}
                    interval={3}
                  />
                  <YAxis 
                    axisLine={false}
                    tickLine={false}
                    tick={{ fontSize: 10, fill: '#64748b' }}
                  />
                  <Tooltip 
                    contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                  />
                  <Legend verticalAlign="top" align="right" iconType="circle" wrapperStyle={{ fontSize: '12px', paddingBottom: '20px' }} />
                  <Area 
                    type="monotone" 
                    dataKey="Required" 
                    stroke="#6366f1" 
                    strokeWidth={2}
                    fillOpacity={1} 
                    fill="url(#colorRequired)" 
                  />
                  <Area 
                    type="monotone" 
                    dataKey="Assigned" 
                    stroke="#10b981" 
                    strokeWidth={2}
                    fillOpacity={1} 
                    fill="url(#colorAssigned)" 
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      )}

      <Card className="border-none shadow-sm bg-blue-50/30 border border-blue-100/50">
        <CardContent className="p-4 flex items-start gap-4">
          <div className="w-10 h-10 rounded-full bg-blue-100 flex items-center justify-center shrink-0 mt-1">
            <Sparkles className="w-5 h-5 text-blue-600" />
          </div>
          <div className="space-y-1">
            <h3 className="font-bold text-blue-900 text-sm">Roster Insights & Logic</h3>
            <p className="text-xs text-blue-700 leading-relaxed max-w-4xl">
              Our AI-powered engine prioritizes <strong>Forecast Requirements</strong> to ensure optimal coverage. 
              The system automatically enforces <strong>Forward Rotation</strong> (Morning → Afternoon → Night) to maintain staff health, 
              while balancing working days against your monthly reference targets. 
              <span className="block mt-1 opacity-80 italic">Note: If supply is lower than demand, the system will highlight coverage gaps in the Capacity Overview.</span>
            </p>
          </div>
        </CardContent>
      </Card>

      {roster.length > 0 ? (
        <Card className="border-none shadow-sm overflow-hidden">
          <CardHeader className="border-b border-slate-100 flex flex-row items-center justify-between space-y-0 py-4">
            <CardTitle className="text-lg flex items-center gap-2">
              {viewMode === 'grid' ? (
                <>
                  <Users className="w-5 h-5 text-primary" />
                  Monthly Roster Grid
                </>
              ) : (
                <>
                  <CalendarIcon className="w-5 h-5 text-primary" />
                  Personal Roster Calendar
                </>
              )}
            </CardTitle>
            {viewMode === 'calendar' && (
              <div className="flex items-center gap-2">
                <span className="text-xs text-slate-500 font-medium">Select Employee:</span>
                <select 
                  className="text-xs border rounded-md p-1 bg-white outline-none focus:ring-1 ring-indigo-500"
                  value={selectedEmployeeId || ''}
                  onChange={(e) => setSelectedEmployeeId(e.target.value)}
                >
                  {roster.map(emp => (
                    <option key={emp.employeeId} value={emp.employeeId}>{emp.employeeName}</option>
                  ))}
                </select>
              </div>
            )}
          </CardHeader>
          <CardContent className="p-0">
            {viewMode === 'grid' ? (
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader className="bg-slate-50">
                  <TableRow>
                    <TableHead className="min-w-[150px] sticky left-0 bg-slate-50 z-20">Employee</TableHead>
                    <TableHead className="min-w-[100px] text-center border-r bg-slate-50/50">Summary (W/O)</TableHead>
                    <TableHead className="min-w-[120px] text-center border-r bg-slate-50/50">Shift Mix</TableHead>
                    {volumeData.map(day => (
                      <TableHead key={day.date} className="min-w-[80px] text-center">
                        <div className="flex flex-col items-center">
                          <span className="text-[10px] font-bold">{day.date.split('-').slice(1).join('/')}</span>
                          <span className="text-[9px] text-slate-500 uppercase">{day.day.substring(0, 3)}</span>
                        </div>
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredRoster.map(emp => (
                    <TableRow key={emp.employeeId} className="hover:bg-slate-50/50 transition-colors">
                      <TableCell className="font-medium text-slate-900 sticky left-0 bg-white z-10">
                        <div className="flex flex-col">
                          <span className="text-sm">{emp.employeeName}</span>
                          <span className="text-[10px] text-slate-400 font-mono">{emp.nip}</span>
                        </div>
                      </TableCell>
                      <TableCell className="text-center border-r bg-slate-50/30">
                        <div className="flex flex-col gap-1.5 py-1">
                          <div className="flex items-center justify-center gap-1">
                            <Badge 
                              variant={emp.totalWorkingDays !== emp.targetWorkingDays ? "destructive" : "secondary"}
                              className="text-[10px] px-1.5 h-5 font-bold"
                            >
                              {emp.totalWorkingDays} / {emp.targetWorkingDays} W
                            </Badge>
                          </div>
                          <div className="flex items-center justify-center">
                            <Badge 
                              variant={emp.totalOffDays !== emp.targetOffDays ? "destructive" : "outline"}
                              className={`text-[10px] px-1.5 h-5 font-bold ${emp.totalOffDays === emp.targetOffDays ? 'bg-slate-100 text-slate-600 border-slate-200' : ''}`}
                            >
                              {emp.totalOffDays} / {emp.targetOffDays} O
                            </Badge>
                          </div>
                        </div>
                      </TableCell>
                      <TableCell className="text-center border-r bg-slate-50/10 min-w-[120px]">
                        <div className="flex flex-col gap-2 px-2">
                          <div className="flex h-2 w-full bg-slate-100 rounded-full overflow-hidden border border-slate-200">
                            {shiftCodes.map((sc, idx) => {
                              const count = Object.values(emp.days).filter(d => d === sc.code).length;
                              if (count === 0) return null;
                              const colors = ['bg-blue-500', 'bg-emerald-500', 'bg-amber-500', 'bg-purple-500', 'bg-pink-500', 'bg-cyan-500'];
                              const color = colors[idx % colors.length];
                              return (
                                <div 
                                  key={sc.code}
                                  className={`${color} h-full border-r border-white/20 last:border-0`}
                                  style={{ width: `${(count / volumeData.length) * 100}%` }}
                                  title={`${sc.code}: ${count} days`}
                                />
                              );
                            })}
                            {(() => {
                              const offCount = Object.values(emp.days).filter(d => d === 'OFF').length;
                              if (offCount === 0) return null;
                              return (
                                <div 
                                  className="bg-slate-200 h-full"
                                  style={{ width: `${(offCount / volumeData.length) * 100}%` }}
                                  title={`OFF: ${offCount} days`}
                                />
                              );
                            })()}
                          </div>
                          <div className="flex flex-wrap justify-center gap-x-2 gap-y-0.5">
                            {shiftCodes.map((sc, idx) => {
                              const count = Object.values(emp.days).filter(d => d === sc.code).length;
                              if (count === 0) return null;
                              const colors = ['text-blue-600', 'text-emerald-600', 'text-amber-600', 'text-purple-600', 'text-pink-600', 'text-cyan-600'];
                              return (
                                <span key={sc.code} className={`text-[9px] font-bold ${colors[idx % colors.length]}`}>
                                  {sc.code}:{count}
                                </span>
                              );
                            })}
                          </div>
                        </div>
                      </TableCell>
                      {volumeData.map((day, dayIdx) => {
                        const shift = emp.days[day.date] || 'OFF';
                        const prevDayDate = dayIdx > 0 ? volumeData[dayIdx - 1].date : null;
                        const prevShiftCode = prevDayDate ? emp.days[prevDayDate] : null;
                        
                        let restViolation = false;
                        let restHours = 0;
                        let consecViolation = false;
                        let consecDays = 0;

                        if (shift !== 'OFF') {
                          // Rest Violation check
                          if (prevShiftCode && prevShiftCode !== 'OFF') {
                            const sPrev = shiftCodes.find(sc => sc.code === prevShiftCode);
                            const sCurr = shiftCodes.find(sc => sc.code === shift);
                            if (sPrev && sCurr) {
                              const restMins = getRestHours(sPrev, sCurr);
                              if (restMins < 11 * 60) {
                                restViolation = true;
                                restHours = Math.round(restMins / 60 * 10) / 10;
                              }
                            }
                          }

                          // Consecutive Day Violation check (6-day compliance)
                          let count = 1;
                          for (let i = dayIdx - 1; i >= 0; i--) {
                            const d = volumeData[i].date;
                            if (emp.days[d] && emp.days[d] !== 'OFF') count++;
                            else break;
                          }
                          if (count >= 6) {
                            consecViolation = true;
                            consecDays = count;
                          }
                        }

                        return (
                          <TableCell key={day.date} className="text-center p-1">
                            <div className="relative inline-block">
                              <Badge 
                                variant="outline" 
                                className={`text-[10px] px-1 h-6 min-w-[40px] justify-center font-bold ${getShiftBadgeClasses(shift, restViolation, consecViolation)}`}
                                title={
                                  restViolation ? `Rest Violation: Only ${restHours}h rest` : 
                                  consecViolation ? `6-Day Compliance Violation: ${consecDays} consecutive days` : 
                                  undefined
                                }
                              >
                                {shift}
                              </Badge>
                              {(restViolation || consecViolation) && (
                                <div className="absolute -top-1 -right-1 bg-white rounded-full">
                                  <AlertCircle className={`w-3 h-3 ${restViolation ? 'text-red-500' : 'text-orange-500'} fill-white`} />
                                </div>
                              )}
                            </div>
                          </TableCell>
                        );
                      })}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          ) : (
            selectedEmployeeId && roster.find(e => e.employeeId === selectedEmployeeId) && (
              <PersonalCalendar employee={roster.find(e => e.employeeId === selectedEmployeeId)!} />
            )
          )}
        </CardContent>
      </Card>
      ) : (
        <div className="h-[400px] flex flex-col items-center justify-center bg-white rounded-xl border border-dashed border-slate-200 text-slate-400 space-y-4">
          <CalendarIcon className="w-12 h-12 opacity-20" />
          <div className="text-center">
            <p className="font-medium">No roster generated yet</p>
            <p className="text-sm">Click "Generate Balanced Roster" to create a schedule.</p>
          </div>
        </div>
      )}
    </div>
  );
}

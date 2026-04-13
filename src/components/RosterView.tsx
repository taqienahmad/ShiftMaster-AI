import React, { useState, useEffect } from 'react';
import { db, collection, onSnapshot, query, setDoc, doc, OperationType, handleFirestoreError } from '../lib/firebase';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Calendar as CalendarIcon, Users, Download, RefreshCw, CheckCircle2, AlertCircle, Clock, Sparkles } from 'lucide-react';
import { toast } from 'sonner';
import { format, startOfWeek, addDays, isSameDay } from 'date-fns';
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
      setVolumeData(list.sort((a, b) => a.date.localeCompare(b.date)));
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
      const dailyRequirements: Record<string, Record<string, number>> = {};
      volumeData.forEach(day => {
        const headcount = adjustedDailyHeadcounts[day.date];
        const shiftSuggestions: Record<string, number> = {};
        const shiftVolumes: Record<string, number> = {};
        let totalShiftVolume = 0;

        shiftCodes.forEach(code => {
          let coveredVolume = 0;
          Object.entries(day.intervals).forEach(([interval, val]) => {
            const volume = Number(val) || 0;
            if (isIntervalInShift(interval, code.startTime, code.endTime)) {
              coveredVolume += volume;
            }
          });
          shiftVolumes[code.code] = coveredVolume;
          totalShiftVolume += coveredVolume;
          shiftSuggestions[code.code] = 0;
        });

        const shiftsWithDemand = shiftCodes.filter(c => shiftVolumes[c.code] > 0);
        let minAssigned = 0;
        if (headcount >= shiftsWithDemand.length) {
          shiftsWithDemand.forEach(code => {
            shiftSuggestions[code.code] = 1;
            minAssigned++;
          });
        }

        const remaining = headcount - minAssigned;
        if (remaining > 0 && totalShiftVolume > 0) {
          shiftCodes.forEach(code => {
            const proportion = shiftVolumes[code.code] / totalShiftVolume;
            shiftSuggestions[code.code] += Math.floor(remaining * proportion);
          });
          let currentTotal = Object.values(shiftSuggestions).reduce((a, b) => a + b, 0);
          let leftover = headcount - currentTotal;
          const sortedShifts = [...shiftCodes].sort((a, b) => shiftVolumes[b.code] - shiftVolumes[a.code]);
          
          while (leftover > 0) {
            for (const code of sortedShifts) {
              if (leftover === 0) break;
              shiftSuggestions[code.code]++;
              leftover--;
            }
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

      // 6. Assign shifts day by day with strict target enforcement, "No Jumping Shift", and "Shift Balancing" logic
      volumeData.forEach((day, dayIdx) => {
        const reqs = { ...dailyRequirements[day.date] };
        const prevDayDate = dayIdx > 0 ? volumeData[dayIdx - 1].date : null;
        const remainingDaysInMonth = volumeData.length - dayIdx;
        
        const availableEmployees = [...newRoster].sort((a, b) => {
          const aNeeded = a.targetWorkingDays - a.totalWorkingDays;
          const bNeeded = b.targetWorkingDays - b.totalWorkingDays;
          if (aNeeded !== bNeeded) return bNeeded - aNeeded;
          return a.totalWorkingDays - b.totalWorkingDays;
        });
        
        shiftCodes.forEach(code => {
          let needed = reqs[code.code] || 0;
          while (needed > 0) {
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

            // Priority 1: Under consecutive limit AND Under target
            let candidates = availableEmployees.filter(emp => 
              !emp.days[day.date] &&
              getConsecutive(emp) < 5 &&
              emp.totalWorkingDays < emp.targetWorkingDays
            );

            // Priority 2: Under consecutive limit (even if at/over target)
            // This prioritizes staff health over precision in the first pass
            if (candidates.length < needed) {
              const healthy = availableEmployees.filter(emp => 
                !emp.days[day.date] &&
                getConsecutive(emp) < 5
              );
              healthy.forEach(h => {
                if (!candidates.find(can => can.employeeId === h.employeeId)) {
                  candidates.push(h);
                }
              });
            }

            // Priority 3: Anyone else (Last resort - breaks 5-day rule)
            if (candidates.length < needed) {
              const desperate = availableEmployees.filter(emp => !emp.days[day.date]);
              desperate.forEach(d => {
                if (!candidates.find(can => can.employeeId === d.employeeId)) {
                  candidates.push(d);
                }
              });
            }

            if (candidates.length === 0) break;

            // Sort by "Hard" and "Soft" Constraints
            candidates.sort((a, b) => {
              // 1. Max 5 Consecutive Days (6-Day Compliance) - ABSOLUTE PRIORITY
              const aCons = getConsecutive(a);
              const bCons = getConsecutive(b);
              const aConsOk = aCons < 5;
              const bConsOk = bCons < 5;
              if (aConsOk !== bConsOk) return aConsOk ? -1 : 1;

              // 2. Monthly Precision (Criticality)
              const aNeeded = a.targetWorkingDays - a.totalWorkingDays;
              const bNeeded = b.targetWorkingDays - b.totalWorkingDays;
              const aIsCritical = aNeeded >= remainingDaysInMonth;
              const bIsCritical = bNeeded >= remainingDaysInMonth;
              if (aIsCritical && !bIsCritical) return -1;
              if (!aIsCritical && bIsCritical) return 1;
              
              // 3. Rest Period & Jumping Shift (Forward Rotation) - NEW PRIORITY
              let aRestOk = true;
              let bRestOk = true;
              let aIsJump = false;
              let bIsJump = false;

              if (prevDayDate) {
                const aPrev = a.days[prevDayDate];
                const bPrev = b.days[prevDayDate];
                
                if (aPrev && aPrev !== 'OFF') {
                  const s = shiftCodes.find(sc => sc.code === aPrev);
                  if (s) {
                    if (getRestHours(s, code) < 11 * 60) aRestOk = false;
                    const [prevH] = s.startTime.split(':').map(Number);
                    const [currH] = code.startTime.split(':').map(Number);
                    if (currH < prevH) aIsJump = true;
                  }
                }
                if (bPrev && bPrev !== 'OFF') {
                  const s = shiftCodes.find(sc => sc.code === bPrev);
                  if (s) {
                    if (getRestHours(s, code) < 11 * 60) bRestOk = false;
                    const [prevH] = s.startTime.split(':').map(Number);
                    const [currH] = code.startTime.split(':').map(Number);
                    if (currH < prevH) bIsJump = true;
                  }
                }
              }
              
              if (aRestOk !== bRestOk) return aRestOk ? -1 : 1;
              if (aIsJump !== bIsJump) return aIsJump ? 1 : -1;

              // 4. Shift Mix Balancing (Balance shifting code each staff)
              const aShiftCount = shiftCounts[a.employeeId][code.code] || 0;
              const bShiftCount = shiftCounts[b.employeeId][code.code] || 0;
              if (aShiftCount !== bShiftCount) return aShiftCount - bShiftCount;

              // 5. Max 2 Consecutive Off Days Priority
              const getConsecutiveOff = (emp: EmployeeRoster) => {
                let count = 0;
                for (let i = dayIdx - 1; i >= 0; i--) {
                  const d = volumeData[i].date;
                  if (emp.days[d] === 'OFF') count++;
                  else break;
                }
                return count;
              };
              const aOff = getConsecutiveOff(a);
              const bOff = getConsecutiveOff(b);
              if (aOff >= 2 && bOff < 2) return -1;
              if (aOff < 2 && bOff >= 2) return 1;

              return (a.targetWorkingDays - a.totalWorkingDays) - (b.targetWorkingDays - b.totalWorkingDays);
            });

            const selected = candidates[0];
            selected.days[day.date] = code.code;
            selected.totalWorkingDays++;
            shiftCounts[selected.employeeId][code.code] = (shiftCounts[selected.employeeId][code.code] || 0) + 1;
            needed--;
            
            const idx = availableEmployees.indexOf(selected);
            if (idx > -1) availableEmployees.splice(idx, 1);
          }
        });

        // Everyone else is OFF
        newRoster.forEach(emp => {
          if (!emp.days[day.date]) {
            emp.days[day.date] = 'OFF';
            emp.totalOffDays++;
          }
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
    const headers = ['NIP', 'Name', ...dates, 'Total Work', 'Total Off'];
    const rows = roster.map(emp => [
      emp.nip,
      emp.employeeName,
      ...dates.map(d => emp.days[d] || 'OFF'),
      emp.totalWorkingDays,
      emp.totalOffDays
    ]);
    const csvContent = [headers, ...rows].map(r => r.join(';')).join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `roster_${format(new Date(), 'yyyyMMdd')}.csv`;
    a.click();
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-slate-900">Roster Management</h1>
          <p className="text-slate-500">Generate and manage balanced employee schedules.</p>
        </div>
        <div className="flex items-center gap-2">
          {isAdmin && (
            <Button onClick={generateBalancedRoster} disabled={loading} className="gap-2 bg-indigo-600 hover:bg-indigo-700 text-white">
              {loading ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Sparkles className="w-4 h-4" />}
              Generate Balanced Roster
            </Button>
          )}
          <Button variant="outline" onClick={downloadRoster} disabled={roster.length === 0} className="gap-2">
            <Download className="w-4 h-4" />
            Export CSV
          </Button>
        </div>
      </div>

      <Card className="border-none shadow-sm bg-blue-50/50 border border-blue-100">
        <CardContent className="p-6">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="flex gap-4">
              <div className="w-10 h-10 rounded-full bg-blue-100 flex items-center justify-center shrink-0">
                <Clock className="w-5 h-5 text-blue-600" />
              </div>
              <div className="space-y-1">
                <h3 className="font-bold text-blue-900 text-sm">Balanced Rostering Logic</h3>
                <p className="text-xs text-blue-700 leading-relaxed">
                  The algorithm prioritizes <strong>Forecast Requirements</strong> first. 
                  If your total staff capacity (Employees × Working Days) is lower than the demand, the roster will be understaffed.
                </p>
              </div>
            </div>

            {roster.length > 0 && (
              <>
                <div className="flex flex-col justify-center space-y-2 px-6 border-l border-blue-100">
                  <div className="flex justify-between items-center">
                    <span className="text-xs font-medium text-slate-500">Total Demand (Forecast)</span>
                    <span className="text-sm font-bold text-slate-900">
                      {totalDemand}
                    </span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-xs font-medium text-slate-500">Total Supply (Capacity)</span>
                    <span className="text-sm font-bold text-blue-600">
                      {roster.length * (roster[0]?.targetWorkingDays || 0)}
                    </span>
                  </div>
                  <div className="pt-1">
                    <div className="w-full bg-slate-200 h-1.5 rounded-full overflow-hidden">
                      <div 
                        className={`h-full ${
                          (roster.length * (roster[0]?.targetWorkingDays || 0)) < totalDemand
                            ? 'bg-amber-500'
                            : 'bg-emerald-500'
                        }`}
                        style={{ 
                          width: `${Math.min(100, (roster.length * (roster[0]?.targetWorkingDays || 0)) / (totalDemand || 1) * 100)}%` 
                        }}
                      />
                    </div>
                  </div>
                </div>

                <div className="flex gap-8 px-6 py-2 bg-white/50 rounded-xl border border-blue-100/50">
                  <div className="text-center">
                    <p className="text-[10px] uppercase tracking-wider text-slate-500 font-bold">Monthly Target</p>
                    <p className="text-xl font-bold text-blue-900">
                      {roster[0]?.targetWorkingDays}W / {roster[0]?.targetOffDays}O
                    </p>
                  </div>
                  <div className="text-center">
                    <p className="text-[10px] uppercase tracking-wider text-slate-500 font-bold">Roster Precision</p>
                    <p className={`text-xl font-bold ${
                      roster.every(e => e.totalWorkingDays === e.targetWorkingDays && e.totalOffDays === e.targetOffDays) 
                        ? 'text-emerald-500' 
                        : 'text-amber-500'
                    }`}>
                      {Math.round((roster.filter(e => e.totalWorkingDays === e.targetWorkingDays && e.totalOffDays === e.targetOffDays).length / roster.length) * 100)}%
                    </p>
                  </div>
                  <div className="text-center">
                    <p className="text-[10px] uppercase tracking-wider text-slate-500 font-bold">6-Day Compliance</p>
                    <p className={`text-xl font-bold ${
                      (() => {
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
                      })() ? 'text-emerald-500' : 'text-orange-500'
                    }`}>
                      {(() => {
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
                      })()}%
                    </p>
                  </div>
                  <div className="text-center">
                    <p className="text-[10px] uppercase tracking-wider text-slate-500 font-bold">Staff Health</p>
                    <p className={`text-xl font-bold ${
                      (roster.reduce((acc, curr) => acc + curr.totalWorkingDays, 0) / roster.length) > roster[0].targetWorkingDays 
                        ? 'text-red-500' 
                        : 'text-emerald-500'
                    }`}>
                      {(roster.reduce((acc, curr) => acc + curr.totalWorkingDays, 0) / roster.length) > roster[0].targetWorkingDays ? 'Overloaded' : 'Balanced'}
                    </p>
                  </div>
                </div>
              </>
            )}
          </div>
        </CardContent>
      </Card>

      {roster.length > 0 ? (
        <Card className="border-none shadow-sm overflow-hidden">
          <CardHeader className="border-b border-slate-100">
            <CardTitle className="text-lg flex items-center gap-2">
              <Users className="w-5 h-5 text-primary" />
              Monthly Roster Grid
            </CardTitle>
          </CardHeader>
          <CardContent className="p-0">
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
                  {roster.map(emp => (
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

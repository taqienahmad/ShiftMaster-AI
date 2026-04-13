import React, { useState, useEffect } from 'react';
import { db, collection, onSnapshot, query, setDoc, doc, deleteDoc, OperationType, handleFirestoreError } from '../lib/firebase';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from '@/components/ui/dialog';
import { Upload, Download, Trash2, Calendar, BarChart3, TrendingUp } from 'lucide-react';
import { toast } from 'sonner';

interface ForecastVolume {
  date: string;
  day: string;
  totalVolume: number;
  intervals: Record<string, number>;
}

export default function ForecastVolume({ isAdmin }: { isAdmin: boolean }) {
  const [volumes, setVolumes] = useState<ForecastVolume[]>([]);
  const [isClearDialogOpen, setIsClearDialogOpen] = useState(false);

  useEffect(() => {
    const q = query(collection(db, 'forecastVolume'));
    const unsubscribe = onSnapshot(q, (snapshot) => {
      const list = snapshot.docs.map(doc => doc.data() as ForecastVolume);
      setVolumes(list.sort((a, b) => a.date.localeCompare(b.date)));
    }, (error) => {
      handleFirestoreError(error, OperationType.LIST, 'forecastVolume');
    });
    return () => unsubscribe();
  }, []);

  const downloadTemplate = () => {
    const csvContent = "sep=;\nDates;01/04/2025;02/04/2025\nDays;Tue;Wed\nTotal Volume;62;67\n0:00 - 1:00;9;9\n1:00 - 2:00;6;8";
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'forecast_volume_template.csv';
    a.click();
    window.URL.revokeObjectURL(url);
  };

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !isAdmin) return;

    const reader = new FileReader();
    reader.onload = async (event) => {
      try {
        let text = event.target?.result as string;
        text = text.replace(/^\ufeff/, '');
        
        let lines = text.split(/\r?\n/).filter(line => line.trim() !== '');
        if (lines.length === 0) return;

        if (lines[0].startsWith('sep=')) {
          lines = lines.slice(1);
        }

        // Detect separator (comma, semicolon, or tab)
        const firstLine = lines[0];
        const counts = {
          ',': (firstLine.match(/,/g) || []).length,
          ';': (firstLine.match(/;/g) || []).length,
          '\t': (firstLine.match(/\t/g) || []).length
        };
        const separator = (Object.keys(counts) as Array<keyof typeof counts>).reduce((a, b) => counts[a] > counts[b] ? a : b);
        
        console.log('Detected separator:', separator);

        const dataMatrix = lines.map(line => line.split(separator).map(v => v.trim()));

        if (dataMatrix.length < 4) {
          toast.error('Invalid format. Need at least Dates, Days, Total Volume, and one interval.');
          return;
        }

        const datesRow = dataMatrix[0];
        const daysRow = dataMatrix[1];
        const totalVolumeRow = dataMatrix[2];
        const intervalRows = dataMatrix.slice(3);

        if (!datesRow[0].toLowerCase().includes('date')) {
          toast.error('First row must start with "Dates"');
          console.log('First row first cell:', datesRow[0]);
          return;
        }

        let successCount = 0;
        let errorCount = 0;

        // Iterate through columns (starting from index 1)
        for (let col = 1; col < datesRow.length; col++) {
          const rawDate = datesRow[col];
          if (!rawDate || rawDate.toLowerCase() === 'dates') continue;

          try {
            // Convert DD/MM/YYYY to YYYY-MM-DD
            let dateStr = '';
            if (rawDate.includes('/')) {
              const parts = rawDate.split('/');
              if (parts.length === 3) {
                // Handle both DD/MM/YYYY and YYYY/MM/DD
                if (parts[0].length === 4) {
                  dateStr = `${parts[0]}-${parts[1].padStart(2, '0')}-${parts[2].padStart(2, '0')}`;
                } else {
                  dateStr = `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
                }
              }
            } else if (rawDate.includes('-')) {
              const parts = rawDate.split('-');
              if (parts.length === 3) {
                if (parts[0].length === 4) {
                  dateStr = rawDate;
                } else {
                  dateStr = `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
                }
              }
            }

            if (!dateStr) {
              console.warn('Invalid date format in column:', col, rawDate);
              continue;
            }

            const day = daysRow[col] || '';
            const totalVolume = parseFloat(totalVolumeRow[col]?.replace(/,/g, '')) || 0;
            const intervals: Record<string, number> = {};

            intervalRows.forEach(row => {
              const intervalName = row[0];
              if (intervalName && !['dates', 'days', 'total volume'].includes(intervalName.toLowerCase())) {
                const valStr = row[col]?.replace(/,/g, '') || '0';
                const intervalValue = parseFloat(valStr) || 0;
                intervals[intervalName] = intervalValue;
              }
            });

            await setDoc(doc(db, 'forecastVolume', dateStr), {
              date: dateStr,
              day,
              totalVolume,
              intervals
            });
            successCount++;
          } catch (err) {
            console.error('Error processing column:', col, err);
            errorCount++;
          }
        }

        if (successCount > 0) {
          toast.success(`Successfully uploaded ${successCount} days of volume data`);
        } else {
          toast.error('No valid data columns found. Check date format (DD/MM/YYYY).');
        }
        
        if (errorCount > 0) {
          toast.error(`Failed to process ${errorCount} columns.`);
        }
      } catch (err) {
        console.error('File processing error:', err);
        toast.error('Failed to process CSV file');
      }
    };
    reader.readAsText(file);
    e.target.value = '';
  };

  const handleClearAll = async () => {
    if (!isAdmin) return;
    try {
      const promises = volumes.map(v => deleteDoc(doc(db, 'forecastVolume', v.date)));
      await Promise.all(promises);
      toast.success('All volume data cleared');
      setIsClearDialogOpen(false);
    } catch (error) {
      toast.error('Failed to clear data');
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-slate-900">Forecast Volume</h1>
          <p className="text-slate-500">Upload and manage work volume per interval.</p>
        </div>

        {isAdmin && (
          <div className="flex items-center gap-2">
            <Dialog open={isClearDialogOpen} onOpenChange={setIsClearDialogOpen}>
              <DialogTrigger asChild>
                <Button 
                  variant="destructive" 
                  className="gap-2"
                  disabled={volumes.length === 0}
                >
                  <Trash2 className="w-4 h-4" />
                  Clear Data
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Are you absolutely sure?</DialogTitle>
                  <p className="text-sm text-slate-500 py-4">
                    This action will permanently delete all forecast volume records. This cannot be undone.
                  </p>
                </DialogHeader>
                <DialogFooter>
                  <Button variant="outline" onClick={() => setIsClearDialogOpen(false)}>Cancel</Button>
                  <Button variant="destructive" onClick={handleClearAll}>Delete All Data</Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>

            <Button variant="outline" onClick={downloadTemplate} className="gap-2">
              <Download className="w-4 h-4" />
              Template
            </Button>

            <label className="cursor-pointer">
              <Input type="file" accept=".csv" className="hidden" onChange={handleFileUpload} />
              <div className="flex items-center gap-2 px-4 py-2 rounded-md bg-slate-100 hover:bg-slate-200 text-sm font-medium transition-colors">
                <Upload className="w-4 h-4" />
                Upload CSV
              </div>
            </label>
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card className="border-none shadow-sm">
          <CardContent className="p-4 flex items-center gap-4">
            <div className="w-10 h-10 rounded-xl bg-blue-50 flex items-center justify-center">
              <Calendar className="w-5 h-5 text-blue-600" />
            </div>
            <div>
              <p className="text-[10px] uppercase font-bold text-slate-500 tracking-wider">Total Days</p>
              <p className="text-xl font-bold text-slate-900">{volumes.length}</p>
            </div>
          </CardContent>
        </Card>
        <Card className="border-none shadow-sm">
          <CardContent className="p-4 flex items-center gap-4">
            <div className="w-10 h-10 rounded-xl bg-emerald-50 flex items-center justify-center">
              <BarChart3 className="w-5 h-5 text-emerald-600" />
            </div>
            <div>
              <p className="text-[10px] uppercase font-bold text-slate-500 tracking-wider">Avg Volume</p>
              <p className="text-xl font-bold text-slate-900">
                {volumes.length > 0 
                  ? Math.round(volumes.reduce((acc, v) => acc + v.totalVolume, 0) / volumes.length).toLocaleString() 
                  : 0}
              </p>
            </div>
          </CardContent>
        </Card>
        <Card className="border-none shadow-sm">
          <CardContent className="p-4 flex items-center gap-4">
            <div className="w-10 h-10 rounded-xl bg-amber-50 flex items-center justify-center">
              <TrendingUp className="w-5 h-5 text-amber-600" />
            </div>
            <div>
              <p className="text-[10px] uppercase font-bold text-slate-500 tracking-wider">Peak Volume</p>
              <p className="text-xl font-bold text-slate-900">
                {volumes.length > 0 
                  ? Math.max(...volumes.map(v => v.totalVolume)).toLocaleString() 
                  : 0}
              </p>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card className="border-none shadow-sm overflow-hidden">
        <div className="overflow-x-auto">
          <Table>
            <TableHeader className="bg-slate-50">
              <TableRow>
                <TableHead className="w-[150px]">Date</TableHead>
                <TableHead className="w-[100px]">Day</TableHead>
                <TableHead className="w-[150px]">Total Volume</TableHead>
                <TableHead>Interval Breakdown (Sample)</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {volumes.length > 0 ? (
                volumes.map((v) => (
                  <TableRow key={v.date}>
                    <TableCell className="font-medium">
                      <div className="flex items-center gap-2">
                        <Calendar className="w-4 h-4 text-slate-400" />
                        {v.date}
                      </div>
                    </TableCell>
                    <TableCell>{v.day}</TableCell>
                    <TableCell>
                      <div className="flex items-center gap-2 font-bold text-primary">
                        <BarChart3 className="w-4 h-4" />
                        {v.totalVolume.toLocaleString()}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-2">
                        {Object.entries(v.intervals).slice(0, 4).map(([time, val]) => (
                          <div key={time} className="text-xs bg-slate-100 px-2 py-1 rounded">
                            <span className="text-slate-500">{time}:</span> {val}
                          </div>
                        ))}
                        {Object.keys(v.intervals).length > 4 && (
                          <span className="text-xs text-slate-400">+{Object.keys(v.intervals).length - 4} more</span>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={4} className="h-32 text-center text-slate-500">
                    No volume data uploaded yet.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </Card>
    </div>
  );
}

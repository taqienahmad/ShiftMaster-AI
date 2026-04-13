import React, { useState, useEffect } from 'react';
import { db, collection, onSnapshot, query, setDoc, doc, deleteDoc, OperationType, handleFirestoreError } from '../lib/firebase';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Upload, Trash2, Calendar, FileSpreadsheet, Download } from 'lucide-react';
import { toast } from 'sonner';

interface WorkingDayRef {
  id: string;
  month: string; // e.g., "Jan-25"
  totalDays: number;
  workingDays: number;
  weekend: number;
  holiday: number;
}

export default function WorkingDaysReference({ isAdmin }: { isAdmin: boolean }) {
  const [references, setReferences] = useState<WorkingDayRef[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const q = query(collection(db, 'workingDaysRef'));
    const unsubscribe = onSnapshot(q, (snapshot) => {
      const list = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as WorkingDayRef));
      setReferences(list.sort((a, b) => {
        // Simple sort by month/year string for now
        return a.month.localeCompare(b.month);
      }));
    }, (error) => {
      handleFirestoreError(error, OperationType.LIST, 'workingDaysRef');
    });
    return () => unsubscribe();
  }, []);

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !isAdmin) return;

    const reader = new FileReader();
    reader.onload = async (event) => {
      try {
        const text = event.target?.result as string;
        const lines = text.split(/\r?\n/).filter(line => line.trim() !== '');
        
        if (lines.length < 2) {
          toast.error('CSV file is empty or missing data');
          return;
        }

        // Detect separator
        const firstLine = lines[0];
        const separator = firstLine.includes(';') ? ';' : ',';
        
        let successCount = 0;
        // Skip header
        for (let i = 1; i < lines.length; i++) {
          const parts = lines[i].split(separator).map(p => p.trim());
          if (parts.length >= 5) {
            const month = parts[0];
            const data: WorkingDayRef = {
              id: month,
              month: month,
              totalDays: parseInt(parts[1]) || 0,
              workingDays: parseInt(parts[2]) || 0,
              weekend: parseInt(parts[3]) || 0,
              holiday: parseInt(parts[4]) || 0
            };

            await setDoc(doc(db, 'workingDaysRef', data.id), data);
            successCount++;
          }
        }
        
        toast.success(`Successfully uploaded ${successCount} references`);
      } catch (err) {
        toast.error('Failed to process CSV file');
        console.error(err);
      }
    };
    reader.readAsText(file);
    e.target.value = '';
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteDoc(doc(db, 'workingDaysRef', id));
      toast.success('Reference deleted');
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, 'workingDaysRef');
    }
  };

  const downloadTemplate = () => {
    const csvContent = "Bulan;Total Hari;Working Days;Weekend;Holiday\nJan-25;31;22;8;1\nFeb-25;28;20;8;2";
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'working_days_template.csv';
    a.click();
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-slate-900">Working Days Reference</h1>
          <p className="text-slate-500">Upload monthly working days, weekends, and holidays.</p>
        </div>
        
        {isAdmin && (
          <div className="flex items-center gap-2">
            <Button variant="outline" onClick={downloadTemplate} className="gap-2">
              <Download className="w-4 h-4" />
              Template
            </Button>
            <label className="cursor-pointer">
              <Input type="file" accept=".csv" className="hidden" onChange={handleFileUpload} />
              <div className="flex items-center gap-2 px-4 py-2 rounded-md bg-primary text-white text-sm font-medium hover:bg-primary/90 transition-colors">
                <Upload className="w-4 h-4" />
                Upload CSV
              </div>
            </label>
          </div>
        )}
      </div>

      <Card className="border-none shadow-sm overflow-hidden">
        <CardHeader className="bg-white border-b border-slate-100">
          <CardTitle className="text-lg flex items-center gap-2">
            <Calendar className="w-5 h-5 text-primary" />
            Monthly Reference Data
          </CardTitle>
          <CardDescription>This data will be used to balance working days in the roster.</CardDescription>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader className="bg-slate-50/50">
              <TableRow>
                <TableHead>Month</TableHead>
                <TableHead className="text-center">Total Days</TableHead>
                <TableHead className="text-center">Working Days</TableHead>
                <TableHead className="text-center">Weekend</TableHead>
                <TableHead className="text-center">Holiday</TableHead>
                {isAdmin && <TableHead className="text-right">Actions</TableHead>}
              </TableRow>
            </TableHeader>
            <TableBody>
              {references.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={isAdmin ? 6 : 5} className="h-32 text-center text-slate-500">
                    No reference data found. Please upload a CSV.
                  </TableCell>
                </TableRow>
              ) : (
                references.map((ref) => (
                  <TableRow key={ref.id} className="hover:bg-slate-50/50 transition-colors">
                    <TableCell className="font-bold text-slate-900">{ref.month}</TableCell>
                    <TableCell className="text-center">{ref.totalDays}</TableCell>
                    <TableCell className="text-center font-medium text-emerald-600">{ref.workingDays}</TableCell>
                    <TableCell className="text-center">{ref.weekend}</TableCell>
                    <TableCell className="text-center">{ref.holiday}</TableCell>
                    {isAdmin && (
                      <TableCell className="text-right">
                        <Button 
                          variant="ghost" 
                          size="icon" 
                          className="text-slate-400 hover:text-destructive"
                          onClick={() => handleDelete(ref.id)}
                        >
                          <Trash2 className="w-4 h-4" />
                        </Button>
                      </TableCell>
                    )}
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}

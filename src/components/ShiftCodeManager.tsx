import React, { useState, useEffect } from 'react';
import { db, collection, onSnapshot, query, setDoc, doc, deleteDoc, OperationType, handleFirestoreError } from '../lib/firebase';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from '@/components/ui/dialog';
import { Plus, Trash2, Edit2, Clock, Upload, Download } from 'lucide-react';
import { toast } from 'sonner';

export interface ShiftCode {
  id: string;
  code: string;
  name: string;
  startTime: string;
  endTime: string;
}

export default function ShiftCodeManager({ isAdmin }: { isAdmin: boolean }) {
  const [shiftCodes, setShiftCodes] = useState<ShiftCode[]>([]);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [editingCode, setEditingCode] = useState<ShiftCode | null>(null);
  const [formData, setFormData] = useState({
    code: '',
    name: '',
    startTime: '08:00',
    endTime: '16:00'
  });

  useEffect(() => {
    const q = query(collection(db, 'shiftCodes'));
    const unsubscribe = onSnapshot(q, (snapshot) => {
      const list = snapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as ShiftCode));
      setShiftCodes(list.sort((a, b) => a.code.localeCompare(b.code)));
    }, (error) => {
      handleFirestoreError(error, OperationType.LIST, 'shiftCodes');
    });
    return () => unsubscribe();
  }, []);

  const downloadTemplate = () => {
    const csvContent = "sep=;\nCode;Interval\nDay Off;00:00-00:00\nP37;03:00-12:00\nNS;08:00-17:00\nS8;17:00-02:00\nLeave;Annual Leave";
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'shift_code_template.csv';
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
        // Remove BOM if present
        text = text.replace(/^\ufeff/, '');
        
        let lines = text.split(/\r?\n/).filter(line => line.trim() !== '');
        if (lines.length === 0) return;

        // Skip Excel separator hint if present (e.g., "sep=,")
        if (lines[0].startsWith('sep=')) {
          lines = lines.slice(1);
        }

        if (lines.length < 2) {
          toast.error('CSV file is empty or missing data');
          return;
        }

        // Detect separator (comma, semicolon, or tab)
        const firstLine = lines[0];
        const counts = {
          ',': (firstLine.match(/,/g) || []).length,
          ';': (firstLine.match(/;/g) || []).length,
          '\t': (firstLine.match(/\t/g) || []).length
        };
        const separator = (Object.keys(counts) as Array<keyof typeof counts>).reduce((a, b) => counts[a] > counts[b] ? a : b);
        
        const headers = firstLine.split(separator).map(h => h.trim().toLowerCase());
        
        const requiredHeaders = ['code', 'interval'];
        const missingHeaders = requiredHeaders.filter(req => !headers.some(h => h.includes(req)));
        
        if (missingHeaders.length > 0) {
          toast.error(`Missing required columns: ${missingHeaders.join(', ')}`);
          console.log('Detected headers:', headers);
          return;
        }

        let successCount = 0;
        let errorCount = 0;

        for (let i = 1; i < lines.length; i++) {
          const values = lines[i].split(separator).map(v => v.trim());
          const codeIndex = headers.findIndex(h => h.includes('code'));
          const intervalIndex = headers.findIndex(h => h.includes('interval'));
          
          const code = values[codeIndex];
          const interval = values[intervalIndex];

          if (code && interval) {
            try {
              let startTime = '00:00';
              let endTime = '00:00';
              let name = interval;

              if (interval.includes('-')) {
                const times = interval.split('-');
                if (times.length === 2) {
                  startTime = times[0].trim();
                  endTime = times[1].trim();
                  name = `${code} (${interval})`;
                }
              }

              await setDoc(doc(db, 'shiftCodes', code.toUpperCase()), {
                id: code.toUpperCase(),
                code: code.toUpperCase(),
                name,
                startTime,
                endTime
              });
              successCount++;
            } catch (err) {
              console.error('Error uploading shift code:', err);
              errorCount++;
            }
          } else {
            errorCount++;
          }
        }
        
        if (successCount > 0) {
          toast.success(`Successfully uploaded ${successCount} shift codes`);
        }
        if (errorCount > 0) {
          toast.error(`Failed to upload ${errorCount} rows. Check format.`);
        }
      } catch (err) {
        toast.error('Failed to process CSV file');
      }
    };
    reader.readAsText(file);
    // Reset input
    e.target.value = '';
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!isAdmin) return;

    try {
      const id = editingCode?.id || formData.code;
      await setDoc(doc(db, 'shiftCodes', id), formData);
      toast.success(editingCode ? 'Shift code updated' : 'Shift code added');
      setIsDialogOpen(false);
      resetForm();
    } catch (error) {
      handleFirestoreError(error, OperationType.WRITE, 'shiftCodes');
    }
  };

  const resetForm = () => {
    setFormData({ code: '', name: '', startTime: '08:00', endTime: '16:00' });
    setEditingCode(null);
  };

  const handleEdit = (code: ShiftCode) => {
    setEditingCode(code);
    setFormData({
      code: code.code,
      name: code.name,
      startTime: code.startTime,
      endTime: code.endTime
    });
    setIsDialogOpen(true);
  };

  const handleDelete = async (id: string) => {
    if (!isAdmin) return;
    try {
      await deleteDoc(doc(db, 'shiftCodes', id));
      toast.success('Shift code deleted');
    } catch (error) {
      handleFirestoreError(error, OperationType.DELETE, 'shiftCodes');
    }
  };

  const [isClearDialogOpen, setIsClearDialogOpen] = useState(false);

  const handleClearAll = async () => {
    if (!isAdmin) return;
    try {
      const promises = shiftCodes.map(code => deleteDoc(doc(db, 'shiftCodes', code.id)));
      await Promise.all(promises);
      toast.success('All shift codes cleared');
      setIsClearDialogOpen(false);
    } catch (error) {
      toast.error('Failed to clear shift codes');
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-slate-900">Shift Codes</h1>
          <p className="text-slate-500">Define shift patterns and working hours.</p>
        </div>

        {isAdmin && (
          <div className="flex items-center gap-2">
            {shiftCodes.length > 0 && (
              <Dialog open={isClearDialogOpen} onOpenChange={setIsClearDialogOpen}>
                <DialogTrigger asChild>
                  <Button variant="destructive" className="gap-2">
                    <Trash2 className="w-4 h-4" />
                    Clear Data
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Are you absolutely sure?</DialogTitle>
                    <p className="text-sm text-slate-500 py-4">
                      This action will permanently delete all shift code records from the database. This cannot be undone.
                    </p>
                  </DialogHeader>
                  <DialogFooter>
                    <Button variant="outline" onClick={() => setIsClearDialogOpen(false)}>Cancel</Button>
                    <Button variant="destructive" onClick={handleClearAll}>Delete All Data</Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            )}
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
            <Dialog open={isDialogOpen} onOpenChange={(open) => { setIsDialogOpen(open); if (!open) resetForm(); }}>
              <DialogTrigger asChild>
                <Button className="gap-2">
                  <Plus className="w-4 h-4" />
                  Add Shift Code
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>{editingCode ? 'Edit Shift Code' : 'Add New Shift Code'}</DialogTitle>
                </DialogHeader>
                <form onSubmit={handleSubmit} className="space-y-4 pt-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Code (e.g. S1)</label>
                      <Input 
                        required 
                        value={formData.code} 
                        onChange={e => setFormData({...formData, code: e.target.value.toUpperCase()})}
                        disabled={!!editingCode}
                      />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Name</label>
                      <Input 
                        required 
                        value={formData.name} 
                        onChange={e => setFormData({...formData, name: e.target.value})}
                      />
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <label className="text-sm font-medium">Start Time</label>
                      <Input 
                        type="time" 
                        required 
                        value={formData.startTime} 
                        onChange={e => setFormData({...formData, startTime: e.target.value})}
                      />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium">End Time</label>
                      <Input 
                        type="time" 
                        required 
                        value={formData.endTime} 
                        onChange={e => setFormData({...formData, endTime: e.target.value})}
                      />
                    </div>
                  </div>
                  <DialogFooter>
                    <Button type="submit" className="w-full">
                      {editingCode ? 'Update Shift Code' : 'Create Shift Code'}
                    </Button>
                  </DialogFooter>
                </form>
              </DialogContent>
            </Dialog>
          </div>
        )}
      </div>

      <Card className="border-none shadow-sm overflow-hidden">
        <Table>
          <TableHeader className="bg-slate-50">
            <TableRow>
              <TableHead>Code</TableHead>
              <TableHead>Name</TableHead>
              <TableHead>Hours</TableHead>
              {isAdmin && <TableHead className="text-right">Actions</TableHead>}
            </TableRow>
          </TableHeader>
          <TableBody>
            {shiftCodes.length > 0 ? (
              shiftCodes.map((code) => (
                <TableRow key={code.id}>
                  <TableCell className="font-bold text-primary">{code.code}</TableCell>
                  <TableCell>{code.name}</TableCell>
                  <TableCell>
                    <div className="flex items-center gap-2 text-slate-600">
                      <Clock className="w-4 h-4" />
                      {code.startTime} - {code.endTime}
                    </div>
                  </TableCell>
                  {isAdmin && (
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-2">
                        <Button variant="ghost" size="icon" onClick={() => handleEdit(code)}>
                          <Edit2 className="w-4 h-4 text-slate-400" />
                        </Button>
                        <Button variant="ghost" size="icon" onClick={() => handleDelete(code.id)}>
                          <Trash2 className="w-4 h-4 text-red-400" />
                        </Button>
                      </div>
                    </TableCell>
                  )}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={isAdmin ? 4 : 3} className="h-24 text-center text-slate-500">
                  No shift codes defined.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </Card>
    </div>
  );
}

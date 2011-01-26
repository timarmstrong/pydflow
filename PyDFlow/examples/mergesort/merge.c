#include <stdio.h>
#include <stdlib.h>
/* Merges two files of integers
 * */
int main(int argc, char **argv) {
    FILE *in1, *in2, *out;
    int count = 0;
    if (argc != 4) {
        fprintf(stderr, "Requires 3 arguments\n");
        exit(1);
    }
    if (!(in1 = fopen(argv[1], "r")))  {
        fprintf(stderr, "Could not open %s\n", argv[1]);
        exit(1);
    }
    if (!(in2 = fopen(argv[2], "r")))  {
        fprintf(stderr, "Could not open %s\n", argv[2]);
        exit(1);
    }
    if (!(out = fopen(argv[3], "w")))  {
        fprintf(stderr, "Could not open %s\n", argv[3]);
        exit(1);
    }

    int n1, n2;
    FILE *in_rem; // File with remainder of input
    int both_open = 1;
    if (fscanf(in1, "%d", &n1) == 1) {
        if (fscanf(in2, "%d", &n2) == 1) {
            // Both files open - start merging
            while (both_open) {
                if (n1 <= n2) {
                    fprintf(out, "%d\n", n1);
                    count ++;
                    if (fscanf(in1, "%d", &n1) != 1) {
                        both_open = 0;
                        fclose(in1);
                        in_rem = in2;
                    }
                }
                else {
                    fprintf(out, "%d\n", n2);
                    count++;
                    if (fscanf(in2, "%d", &n2) != 1) {
                        both_open = 0;
                        fclose(in2);
                        in_rem = in1;
                    }
                }
            }
        }
        else {
            // only in1 has data
            in_rem = in1;
            fclose(in2);
        }
    }
    else {
        in_rem = in2;
        fclose(in1);
    }
    while (fscanf(in_rem, "%d", &n1) == 1) {
        fprintf(out, "%d\n", n1);
        count++;
    }
    fclose(in_rem);
    fclose(out);
    
    fprintf(stderr, "%d items merged\n", count);

}
